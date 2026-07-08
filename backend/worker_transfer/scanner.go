package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Scanner Worker
// 1. Polls Backend for PENDING TransferJobs
// 2. Lists Source S3/R2
// 3. Batches inserts to Backend
// 4. Updates Status to RUNNING

type TransferJob struct {
	JobID            uint             `json:"job_id"`
	SrcDir           string           `json:"src_dir"`
	DstDir           string           `json:"dst_dir"`
	Include          string           `json:"include"`
	Exclude          string           `json:"exclude"`
	DeleteSource     bool             `json:"delete_source"`
	Metadata         TransferMetadata `json:"metadata"`
	Status           string           `json:"status"`
	PeriodicInterval int              `json:"periodic_interval"`
	IsIncremental    bool             `json:"is_incremental"`
	LastScanTime     *time.Time       `json:"last_scan_time"`
}

type UpdateStatusRequest struct {
	Status        string     `json:"status"`
	LastScanTime  *time.Time `json:"last_scan_time,omitempty"`
	ResultMessage string     `json:"result_message,omitempty"`
}

var (
	PagesScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_pages_scanned_total",
		Help: "Total number of S3 pages scanned",
	})

	TasksDiscovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_tasks_discovered_total",
		Help: "Total number of tasks discovered",
	})
)

func runScanner() {
	loadConfig()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9093")
		http.ListenAndServe(":9093", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	log.Println("Scanner Worker Started")

	httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var activeJobs sync.Map

	for {
		jobs, err := getPendingJobs()
		if err != nil {
			log.Printf("Error getting pending jobs: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		for _, job := range jobs {
			if _, loaded := activeJobs.LoadOrStore(job.JobID, true); loaded {
				continue
			}

			go func(j TransferJob) {
				defer activeJobs.Delete(j.JobID)
				processJob(j)
			}(job)
		}

		time.Sleep(5 * time.Second)
	}
}

func getPendingJobs() ([]TransferJob, error) {
	resp, err := http.Get(apiBaseURL + "/jobs/pending")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var jobs []TransferJob
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func updateJobStatus(jobID uint, status string, lastScanTime *time.Time, msg string) error {
	req := UpdateStatusRequest{Status: status, LastScanTime: lastScanTime, ResultMessage: msg}
	data, _ := json.Marshal(req)

	reqObj, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
	reqObj.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(reqObj)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to update status: %d", resp.StatusCode)
	}
	return nil
}

type TransferTaskInput struct {
	Src  string `json:"src"`
	Size int64  `json:"size"`
}

func processJob(job TransferJob) {
	startTime := time.Now()
	jobJSON, _ := json.Marshal(job)
	log.Printf("Processing Job: %s", string(jobJSON))

	// 1. Update status to RUNNING
	if err := updateJobStatus(job.JobID, "RUNNING", nil, ""); err != nil {
		log.Printf("Failed to set RUNNING for job %d: %v", job.JobID, err)
		return
	}

	// 2. Init S3 Source Client
	s3Client, err := initSourceS3()
	if err != nil {
		log.Printf("Failed to init S3 for job %d: %v", job.JobID, err)
		updateJobStatus(job.JobID, "FAILED", nil, fmt.Sprintf("Init S3 failed: %v", err))
		return
	}

	// 3. List and Batch Insert
	bucketName := getBucketFromEndpoint(cfg.Storage.Src.Endpoint)
	prefix := strings.TrimSpace(job.SrcDir)
	log.Printf("Listing objects for job %d in bucket '%s' with prefix '%s'", job.JobID, bucketName, prefix)

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	count := 0
	skipped := 0
	pages := 0
	lastUpdate := time.Now()

	// Channel for async sending
	taskChan := make(chan TransferTaskInput, 1500)
	var wg sync.WaitGroup
	wg.Add(1)

	// Consumer Goroutine
	go func() {
		defer wg.Done()
		var internalBatch []TransferTaskInput
		for task := range taskChan {
			internalBatch = append(internalBatch, task)
			if len(internalBatch) >= 700 {
				if err := sendBatch(job.JobID, internalBatch); err != nil {
					log.Printf("Failed to send batch for job %d: %v", job.JobID, err)
				}
				internalBatch = nil // Clear
			}
		}
		// Flush remaining
		if len(internalBatch) > 0 {
			if err := sendBatch(job.JobID, internalBatch); err != nil {
				log.Printf("Failed to send final batch for job %d: %v", job.JobID, err)
			}
		}
	}()

	for paginator.HasMorePages() {
		pages++
		PagesScanned.Inc()
		log.Printf("Requesting page %d for job %d...", pages, job.JobID)

		// Retry logic with exponential backoff
		var page *s3.ListObjectsV2Output
		var err error
		maxRetries := 5
		retryCount := 0
		baseDelay := 5 * time.Second

		for retryCount < maxRetries {
			page, err = paginator.NextPage(context.TODO())
			if err == nil {
				break // Success, exit retry loop
			}

			log.Printf("ListObjectsV2 failed for job %d on page %d (attempt %d/%d): %v", job.JobID, pages, retryCount+1, maxRetries, err)

			// Check if the error is due to rate limiting
			errMsg := err.Error()
			isRateLimited := strings.Contains(strings.ToLower(errMsg), "ratelimit") ||
				strings.Contains(strings.ToLower(errMsg), "throttled") ||
				strings.Contains(strings.ToLower(errMsg), "slowdown") ||
				strings.Contains(errMsg, "429")

			if isRateLimited {
				log.Printf("Detected rate limiting for job %d, applying backoff strategy", job.JobID)
				// Use a longer delay for rate limiting
				delay := baseDelay * time.Duration(1<<uint(retryCount)) // Exponential backoff
				if retryCount > 0 {
					log.Printf("Rate limited, waiting for %v before retry %d/%d", delay, retryCount+1, maxRetries)
				}
				time.Sleep(delay)
			} else {
				// Standard error - shorter delay
				delay := baseDelay * time.Duration(1<<uint(retryCount)) / 2
				if delay < baseDelay {
					delay = baseDelay
				}
				log.Printf("Request failed, waiting for %v before retry %d/%d", delay, retryCount+1, maxRetries)
				time.Sleep(delay)
			}

			retryCount++
		}

		if err != nil {
			log.Printf("ListObjectsV2 failed for job %d on page %d after %d retries: %v", job.JobID, pages, maxRetries, err)
			updateJobStatus(job.JobID, "FAILED", nil, fmt.Sprintf("List failed on page %d after %d retries: %v", pages, maxRetries, err))
			close(taskChan) // Ensure consumer stops
			return
		}

		log.Printf("Page %d for job %d contained %d objects.", pages, job.JobID, len(page.Contents))
		for _, obj := range page.Contents {
			key := *obj.Key
			if strings.HasSuffix(key, "/") {
				continue
			}

			// 3.1 Filter Include/Exclude
			match := func(pattern, name string) (bool, error) {
				if strings.Contains(pattern, "/") {
					return path.Match(pattern, name)
				}
				return path.Match(pattern, path.Base(name))
			}

			if job.Include != "" {
				matched, err := match(job.Include, key)
				if err == nil && !matched {
					continue
				}
			}
			if job.Exclude != "" {
				matched, err := match(job.Exclude, key)
				if err == nil && matched {
					continue
				}
			}

			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			taskChan <- TransferTaskInput{Src: key, Size: size}
			TasksDiscovered.Inc()
			count++
		}

		// Periodic Job Update (Heartbeat / Metadata Refresh)
		if time.Since(lastUpdate) > 10*time.Second { // Check every 10s roughly (per page)
			// Refresh Job Config
			latestJob, err := getJob(job.JobID)
			if err == nil {
				// Update filters if changed
				job.Include = latestJob.Include
				job.Exclude = latestJob.Exclude
				job.IsIncremental = latestJob.IsIncremental

				// Optional: Check status. If Cancelled, abort?
				if latestJob.Status != "RUNNING" && latestJob.Status != "PENDING" {
					log.Printf("Job %d status changed to %s. Aborting scan.", job.JobID, latestJob.Status)
					close(taskChan)
					return // Stop scanning
				}
			} else {
				log.Printf("Failed to refresh job %d: %v", job.JobID, err)
			}

			// Update Status / Heartbeat
			msg := fmt.Sprintf("Scanning... Pages: %d, Tasks: %d, Skipped: %d", pages, count, skipped)
			updateJobStatus(job.JobID, "RUNNING", nil, msg)
			lastUpdate = time.Now()
		}

		// Add a small delay between pages to reduce request rate after encountering rate limiting
		// We track whether the last page request was affected by rate limiting
		if pages > 0 {
			// In case we've had a rate limiting event recently, apply a small delay
			// This is a proactive measure to prevent subsequent rate limiting
			standardDelay := 100 * time.Millisecond
			//log.Printf("Waiting %v before requesting next page for job %d to prevent rate limiting", standardDelay, job.JobID)
			time.Sleep(standardDelay)
		}
	}

	close(taskChan) // Signal consumer to finish
	wg.Wait()       // Wait for consumer to finish processing

	log.Printf("Job %d scanned. Total pages: %d. New tasks: %d, Skipped (old): %d", job.JobID, pages, count, skipped)
	resultMsg := fmt.Sprintf("Scanned %d pages. Tasks: %d, Skipped: %d", pages, count, skipped)

	if job.PeriodicInterval > 0 {
		updateJobStatus(job.JobID, "RUNNING", &startTime, resultMsg)
		log.Printf("Job %d is periodic. Next scan in %d seconds.", job.JobID, job.PeriodicInterval)
	} else {
		updateJobStatus(job.JobID, "RUNNING", &startTime, resultMsg)
	}
}

func getJob(jobID uint) (*TransferJob, error) {
	resp, err := http.Get(fmt.Sprintf("%s/jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var job TransferJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

func sendBatch(jobID uint, tasks []TransferTaskInput) error {
	payload := map[string]interface{}{
		"tasks": tasks,
	}
	data, _ := json.Marshal(payload)
	resp, err := httpClient.Post(fmt.Sprintf("%s/jobs/%d/tasks", apiBaseURL, jobID), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Println("job ", jobID, "error body: ", string(body))
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func initSourceS3() (*s3.Client, error) {
	// Normalize endpoint to http/https as AWS SDK BaseEndpoint requires a web URI
	normalized := cfg.Storage.Src.Endpoint
	isS3 := strings.HasPrefix(normalized, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(normalized, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return nil, err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	baseEndpoint := fmt.Sprintf("%s://%s", u.Scheme, host)

	httpTransport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     60 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Storage.Src.AccessKey,
			cfg.Storage.Src.SecretKey,
			"",
		)),
		awsconfig.WithRegion("auto"),
		awsconfig.WithHTTPClient(&http.Client{
			Transport: httpTransport,
			Timeout:   60 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(c, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false
	}), nil
}
