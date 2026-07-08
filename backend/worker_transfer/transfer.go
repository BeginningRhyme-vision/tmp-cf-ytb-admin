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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Transfer Worker
// 1. Acquire Tasks (Transfer)
// 2. Transfer using external service

type TransferTask struct {
	ID     int64  `json:"id"`
	JobID  int64  `json:"job_id"`
	Src    string `json:"src"`
	Size   int64  `json:"size"`
	Status string `json:"status"`
}

type JobInfo struct {
	JobID        uint             `json:"job_id"`
	Metadata     TransferMetadata `json:"metadata"`
	DstDir       string           `json:"dst_dir"`
	SrcDir       string           `json:"src_dir"`
	DeleteSource bool             `json:"delete_source"`
}

type cachedJob struct {
	info   JobInfo
	expiry time.Time
}

type JobStatsDelta struct {
	Success int
	Failed  int
}

var (
	jobCache           sync.Map // JobID -> cachedJob
	httpClient         *http.Client
	transferClient     *http.Client
	workerCount        int
	taskBufferSize     int
	partConcurrency    int
	multipartThreshold int64
	minPartSize        int64

	s3Clients sync.Map // Endpoint -> *s3.Client (Cache for Destinations)
	srcClient *s3.Client

	statsBuffer = make(map[int64]*JobStatsDelta)
	statsMutex  sync.Mutex

	// Metrics
	BytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "transfer_bytes_transferred_total",
		Help: "Total bytes transferred",
	})

	TasksTransferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transfer_tasks_transferred_total",
		Help: "Total tasks processed",
	}, []string{"status"})

	TransferDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "transfer_duration_seconds",
		Help: "Duration of transfers",
	})
)

const (
	WorkerID                    = "go-transfer-1"
	DefaultConcurrentWorkers    = 128
	DefaultTaskBufferSize       = 256
	DefaultPartConcurrency      = 16
	DefaultMultipartThresholdMB = 16
	DefaultMinPartSizeMB        = 8
	DefaultTransferTimeoutSecs  = 300
	DefaultMaxConnections       = 2048
)

func runTransfer() {
	loadConfig()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9094")
		http.ListenAndServe(":9094", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}
	workerCount = getEnvInt("TRANSFER_MAX_WORKERS", DefaultConcurrentWorkers)
	taskBufferSize = getEnvInt("TRANSFER_TASK_BUFFER", DefaultTaskBufferSize)
	partConcurrency = getEnvInt("TRANSFER_PART_CONCURRENCY", DefaultPartConcurrency)
	multipartThreshold = int64(getEnvInt("TRANSFER_MULTIPART_THRESHOLD_MB", DefaultMultipartThresholdMB)) * 1024 * 1024
	minPartSize = int64(getEnvInt("TRANSFER_MIN_PART_SIZE_MB", DefaultMinPartSizeMB)) * 1024 * 1024
	transferTimeoutSecs := getEnvInt("TRANSFER_TIMEOUT_SECS", DefaultTransferTimeoutSecs)
	maxConnections := getEnvInt("TRANSFER_MAX_CONNECTIONS", DefaultMaxConnections)
	httpClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
	transferClient = &http.Client{
		Timeout: time.Duration(transferTimeoutSecs) * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxConnections,
			MaxIdleConnsPerHost: maxConnections,
			IdleConnTimeout:     120 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   8 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	initSourceClient()
	initStatsFlusher()

	log.Println("Transfer Worker Started")

	taskChan := make(chan TransferTask, taskBufferSize)

	// Start Fetcher
	go func() {
		for {
			// Backpressure: if channel is mostly full, wait a bit
			if len(taskChan) >= taskBufferSize-20 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			tasks, err := acquireTasks()
			if err != nil {
				log.Printf("Error acquiring tasks: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(tasks) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("Acquired %d tasks", len(tasks))
			for _, t := range tasks {
				taskChan <- t
			}
		}
	}()

	// Start Workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskChan {
				processTask(t)
			}
		}()
	}
	wg.Wait()
}

func initStatsFlusher() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			flushStats()
		}
	}()
}

func flushStats() {
	statsMutex.Lock()
	if len(statsBuffer) == 0 {
		statsMutex.Unlock()
		return
	}

	snapshot := make(map[int64]JobStatsDelta)
	for k, v := range statsBuffer {
		if v.Success > 0 || v.Failed > 0 {
			snapshot[k] = *v
		}
	}
	// Reset buffer
	statsBuffer = make(map[int64]*JobStatsDelta)
	statsMutex.Unlock()

	for jobID, delta := range snapshot {
		sendJobStatsUpdate(jobID, delta.Success, delta.Failed)
	}
}

func updateJobStats(jobID int64, incSuccess, incFailed int) {
	if incSuccess == 0 && incFailed == 0 {
		return
	}
	statsMutex.Lock()
	defer statsMutex.Unlock()

	if _, ok := statsBuffer[jobID]; !ok {
		statsBuffer[jobID] = &JobStatsDelta{}
	}
	statsBuffer[jobID].Success += incSuccess
	statsBuffer[jobID].Failed += incFailed
}

type UpdateJobStatusRequest struct {
	Status        string     `json:"status,omitempty"`
	LastScanTime  *time.Time `json:"last_scan_time,omitempty"`
	ResultMessage string     `json:"result_message,omitempty"`
	IncSuccess    int        `json:"inc_success,omitempty"`
	IncFailed     int        `json:"inc_failed,omitempty"`
}

func sendJobStatsUpdate(jobID int64, incSuccess, incFailed int) {
	reqPayload := UpdateJobStatusRequest{
		IncSuccess: incSuccess,
		IncFailed:  incFailed,
	}
	data, _ := json.Marshal(reqPayload)

	req, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to update job stats for job %d: %v", jobID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("Failed to update job stats for job %d: status %d", jobID, resp.StatusCode)
	}
}

func processTask(t TransferTask) {
	start := time.Now()

	job, err := getJobInfo(t.JobID)
	if err != nil {
		log.Printf("Failed to get job info for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	srcDir := strings.TrimSpace(job.SrcDir)
	dstDir := strings.TrimSpace(job.DstDir)

	log.Printf("Processing Task %d (Job %d): %s -> %s", t.ID, t.JobID, t.Src, dstDir)

	// 1. Resolve Dst Client
	sk := job.Metadata.SKEncrypted
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}

	dstClient, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, sk)
	if err != nil {
		log.Printf("Dst client init failed for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", "Dst client init failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	// 2. Resolve Paths
	srcKey := t.Src
	var relKey string
	if srcDir != "" {
		if srcKey == srcDir {
			relKey = ""
		} else if strings.HasPrefix(srcKey, srcDir) {
			relKey = strings.TrimPrefix(srcKey, srcDir)
			relKey = strings.TrimPrefix(relKey, "/")
		} else {
			relKey = srcKey
		}
	} else {
		relKey = srcKey
	}

	dstKey := relKey
	if dstDir != "" {
		if dstKey == "" {
			dstKey = dstDir
		} else {
			dstKey = strings.TrimSuffix(dstDir, "/") + "/" + dstKey
		}
	}

	// 3. Get Object Info (Size) from Src
	srcBucket := getBucketFromEndpoint(cfg.Storage.Src.Endpoint)
	size := t.Size

	if size == 0 {
		head, err := srcClient.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(srcKey),
		})
		if err != nil {
			log.Printf("HeadObject failed for %s/%s: %v", srcBucket, srcKey, err)
			updateTaskStatus(t, "FAILED", "HeadObject failed: "+err.Error())
			updateJobStats(t.JobID, 0, 1)
			TasksTransferred.WithLabelValues("failed").Inc()
			return
		}
		size = *head.ContentLength
	}

	// 4. Construct Public/Virtual-Hosted URLs for Transfer Service (Matches r2s3.go logic)
	srcUrl, err := constructVirtualHostURL(cfg.Storage.Src.Endpoint, srcBucket, srcKey)
	if err != nil {
		log.Printf("Failed to construct Src URL for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", "Construct Src URL failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	dstBucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	log.Printf("Task %d: Transferring %d bytes to bucket '%s' key '%s'", t.ID, size, dstBucket, dstKey)

	// 5. Transfer Loop
	err = transferFile(srcUrl, dstClient, dstBucket, dstKey, size, job.Metadata.Endpoint)
	if err != nil {
		log.Printf("Transfer failed for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
	} else {
		log.Printf("Task %d completed successfully", t.ID)

		if job.DeleteSource {
			_, err := srcClient.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(srcBucket),
				Key:    aws.String(srcKey),
			})
			if err != nil {
				log.Printf("Failed to delete source object %s for task %d: %v", srcKey, t.ID, err)
			} else {
				log.Printf("Deleted source object %s for task %d", srcKey, t.ID)
			}
		}

		updateTaskStatus(t, "COMPLETED", "")
		updateJobStats(t.JobID, 1, 0)

		// Metrics
		duration := time.Since(start).Seconds()
		TransferDuration.Observe(duration)
		BytesTransferred.Add(float64(size))
		TasksTransferred.WithLabelValues("success").Inc()
	}
}

func transferFile(srcURL string, dstClient *s3.Client, dstBucket, dstKey string, size int64, dstEndpoint string) error {
	dstUrl, err := constructVirtualHostURL(dstEndpoint, dstBucket, dstKey)
	if err != nil {
		return err
	}

	if size < multipartThreshold {
		_, err = callTransferService(srcURL, dstUrl, size, 0, "", -1)
		return err
	}

	// Multipart
	createOut, err := dstClient.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstBucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return err
	}
	uploadID := *createOut.UploadId

	partSize := calculatePartSize(size)
	numParts := int((size-1)/partSize) + 1

	var completedParts []types.CompletedPart
	var mu sync.Mutex
	var wg sync.WaitGroup
	errAbort := make(chan error, 1)

	sem := make(chan struct{}, partConcurrency)

	for i := 0; i < numParts; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if end >= size {
			end = size - 1
		}
		partNum := int32(i + 1)

		wg.Add(1)
		go func(pNum int32, s, e int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Use clean URLs, no presigning
			etag, err := callTransferService(srcURL, dstUrl, e-s+1, s, uploadID, int(pNum))
			if err != nil {
				select {
				case errAbort <- err:
				default:
				}
				return
			}

			mu.Lock()
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       aws.String(etag),
				PartNumber: aws.Int32(pNum),
			})
			mu.Unlock()
		}(partNum, start, end)
	}

	wg.Wait()

	select {
	case err := <-errAbort:
		dstClient.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		})
		return err
	default:
	}

	// Sort
	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
	}

	_, err = dstClient.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	return err
}

func callTransferService(srcUrl, dstUrl string, size, offset int64, uploadID string, partNum int) (string, error) {
	// Payload matches r2s3 / downloader logic
	// But `r2s3` sent `r2Key` and `s3Url`.
	// We are sending Presigned URLs for both.

	payload := map[string]interface{}{
		"r2Key":      srcUrl,
		"s3Url":      dstUrl,
		"size":       size,
		"offset":     offset,
		"uploadId":   uploadID,
		"partNumber": partNum,
	}

	body, _ := json.Marshal(payload)

	// Retry
	var lastErr error
	for i := 0; i < 5; i++ {
		resp, err := transferClient.Post(cfg.Storage.TransferServiceURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			var res map[string]interface{}
			if err := json.Unmarshal(respBody, &res); err != nil {
				lastErr = fmt.Errorf("decode response failed: %w", err)
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			if etag, ok := res["etag"].(string); ok {
				return etag, nil
			}
			lastErr = fmt.Errorf("etag missing in response")
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}
		lastErr = fmt.Errorf("status %d body %s", resp.StatusCode, string(respBody))
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return "", fmt.Errorf("service call failed: %v", lastErr)
}

func constructVirtualHostURL(endpointStr, bucket, key string) (string, error) {
	// Mimic r2s3.go logic for creating scheme://bucket.host/key

	// Normalize just to parse host/scheme cleanly if needed, but r2s3 uses simple parsing
	normalized := endpointStr
	isS3 := strings.HasPrefix(endpointStr, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(endpointStr, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	// r2s3 style: fmt.Sprintf("%s://%s.%s/%s", u.Scheme, srcCfg.Bucket, u.Host, obj.Key)
	return fmt.Sprintf("%s://%s.%s/%s", u.Scheme, bucket, host, key), nil
}

func initSourceClient() {
	c, err := createS3Client(cfg.Storage.Src.Endpoint, cfg.Storage.Src.AccessKey, cfg.Storage.Src.SecretKey)
	if err != nil {
		log.Fatalf("Failed to init source client: %v", err)
	}
	srcClient = c
}

func createS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	normalized := endpoint
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

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		awsconfig.WithRegion("auto"),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(c, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false
	}), nil
}

func acquireTasks() ([]TransferTask, error) {
	payload := map[string]interface{}{
		"worker_id": WorkerID,
		"limit":     taskBufferSize,
	}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/acquire", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []TransferTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func getJobInfo(jobID int64) (*JobInfo, error) {
	// Check cache
	if val, ok := jobCache.Load(jobID); ok {
		cj := val.(cachedJob)
		if time.Now().Before(cj.expiry) {
			return &cj.info, nil
		}
	}

	resp, err := httpClient.Get(fmt.Sprintf("%s/jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var job TransferJobStruct
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	// Map backend model to JobInfo
	info := JobInfo{
		JobID:        job.JobID,
		Metadata:     job.Metadata,
		DstDir:       job.DstDir,
		SrcDir:       job.SrcDir,
		DeleteSource: job.DeleteSource,
	}

	jobCache.Store(jobID, cachedJob{info: info, expiry: time.Now().Add(1 * time.Minute)})
	return &info, nil
}

// Temporary struct to match backend response for getJobInfo
type TransferJobStruct struct {
	JobID        uint             `json:"job_id"`
	Metadata     TransferMetadata `json:"metadata"`
	DstDir       string           `json:"dst_dir"`
	SrcDir       string           `json:"src_dir"`
	DeleteSource bool             `json:"delete_source"`
}

func updateTaskStatus(t TransferTask, status string, msg string) {
	t.Status = status
	// msg is ignored by backend currently unless we add a field for it,
	// but let's send it anyway? No, backend models.TransferTask doesn't have Msg.
	// We can ignore msg for now or log it.

	payload := []TransferTask{t}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/update", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Failed to update status for task %d: %v", t.ID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("Failed to update status for task %d: status %d", t.ID, resp.StatusCode)
	}
}

func calculatePartSize(size int64) int64 {
	// Max parts: 10000
	partSize := size / 10000
	if partSize < minPartSize {
		partSize = minPartSize
	}
	return partSize
}

func getEnvInt(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultValue
	}
	return n
}
