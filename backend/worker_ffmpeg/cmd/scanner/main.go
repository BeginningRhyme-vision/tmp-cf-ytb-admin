package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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
	"github.com/redis/go-redis/v9"

	"worker_ffmpeg/common"
)

const (
	MaxRetries = 3
)

var (
	apiBaseURL string
	rdb        *redis.Client
	taskQueue  string
	failedQueue string
	dedupPrefix string
	httpClient *http.Client
	permFailureKey string
	providerGuard string
	requirePrivateEndpoint bool
	internalEndpointKeywords []string
	PagesScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_pages_scanned_total",
		Help: "Total number of S3 pages scanned",
	})

	TasksDiscovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_tasks_discovered_total",
		Help: "Total number of tasks discovered",
	})

	TasksRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_tasks_retried_total",
		Help: "Total number of tasks retried",
	})

	TasksPermanentlyFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_tasks_permanently_failed_total",
		Help: "Total number of tasks that have permanently failed",
	})
)

func startRetryManager() {
	log.Println("Starting FFmpeg Retry Manager")
	for {
		// BLPop from the failed queue
		res, err := rdb.BLPop(context.Background(), 10*time.Second, failedQueue).Result()
		if err != nil {
			if err != redis.Nil {
				log.Printf("RetryManager: Redis error: %v", err)
				time.Sleep(2 * time.Second)
			}
			continue
		}

		var task common.FfmpegTask
		if err := json.Unmarshal([]byte(res[1]), &task); err != nil {
			log.Printf("RetryManager: Failed to unmarshal failed task: %v. Discarding.", err)
			continue
		}

		if task.RetryCount >= MaxRetries {
			log.Printf("Task %d (Job %d) has reached max retries (%d). Marking as permanent failure.", task.ID, task.JobID, MaxRetries)

			// Report permanent failure to backend
			reportResultPatch(task.JobID, false) // This increments failed_count on the job

			// Add to a permanent failure set in Redis to prevent re-processing if scanner finds it again (though it shouldn't)
			rdb.SAdd(context.Background(), permFailureKey, task.ID)
			TasksPermanentlyFailed.Inc()
			continue
		}

		log.Printf("Retrying task %d (Job %d), attempt %d", task.ID, task.JobID, task.RetryCount+1)
		task.Status = "PENDING"
		task.ErrorMessage = "" // Clear previous error

		requeueData, err := json.Marshal(task)
		if err != nil {
			log.Printf("RetryManager: Failed to marshal task %d for requeue: %v", task.ID, err)
			continue
		}

		// Push back to the main pending queue
		if err := rdb.RPush(context.Background(), taskQueue, requeueData).Err(); err != nil {
			log.Printf("RetryManager: Failed to requeue task %d: %v", task.ID, err)
			// If requeue fails, push it back to the failed queue to try again later
			rdb.LPush(context.Background(), failedQueue, res[1])
			time.Sleep(1 * time.Second)
		} else {
			TasksRetried.Inc()
		}
	}
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9093")
		http.ListenAndServe(":9093", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}
	initQueueConfig()

	initRedis()

	httpClient = &http.Client{
		Timeout: 20 * time.Second,
	}

	// Start the retry manager in a separate goroutine
	go startRetryManager()

	log.Println("FFmpeg Scanner Started")

	var activeJobs sync.Map

	for {
		jobs, err := getPendingJobs()
		if err != nil {
			log.Printf("Error getting pending jobs: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, job := range jobs {
			if _, loaded := activeJobs.LoadOrStore(job.ID, true); loaded {
				continue
			}

			go func(j common.FfmpegJob) {
				defer activeJobs.Delete(j.ID)
				processJob(j)
			}(job)
		}

		time.Sleep(5 * time.Second)
	}
}

func initQueueConfig() {
	queuePrefix := strings.TrimSpace(os.Getenv("FFMPEG_QUEUE_PREFIX"))
	if queuePrefix == "" {
		queuePrefix = "ffmpeg"
	}
	queuePrefix = strings.Trim(queuePrefix, ":")
	taskQueue = fmt.Sprintf("queue:%s:pending", queuePrefix)
	failedQueue = fmt.Sprintf("queue:%s:failed", queuePrefix)
	dedupPrefix = fmt.Sprintf("queue:%s:dedup:", queuePrefix)
	permFailureKey = fmt.Sprintf("%s:task:perm_failure", queuePrefix)
	log.Printf("FFmpeg queue config initialized: prefix=%s task=%s failed=%s", queuePrefix, taskQueue, failedQueue)
	privateFlag := strings.TrimSpace(os.Getenv("FFMPEG_REQUIRE_PRIVATE_ENDPOINT"))
	if privateFlag == "" {
		requirePrivateEndpoint = true
	} else {
		requirePrivateEndpoint = strings.EqualFold(privateFlag, "true")
	}
	rawKeywords := strings.TrimSpace(os.Getenv("FFMPEG_INTERNAL_ENDPOINT_KEYWORDS"))
	if rawKeywords == "" {
		rawKeywords = "internal,intranet,private,privatelink,vpc,aliyuncs,ivolces,tos-s3"
	}
	providerGuard = strings.ToLower(strings.TrimSpace(os.Getenv("PROVIDER")))
	internalEndpointKeywords = nil
	for _, k := range strings.Split(rawKeywords, ",") {
		k = strings.ToLower(strings.TrimSpace(k))
		if k != "" {
			internalEndpointKeywords = append(internalEndpointKeywords, k)
		}
	}
	log.Printf("FFmpeg endpoint guard: require_private=%v keywords=%v", requirePrivateEndpoint, internalEndpointKeywords)
}

func reportResultPatch(jobID int64, success bool) {
	req := common.UpdateJobStatusRequest{}
	if success {
		req.IncSuccess = 1
	} else {
		req.IncFailed = 1
	}
	data, _ := json.Marshal(req)

	reqObj, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/ffmpeg-jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
	reqObj.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(reqObj)
	if err != nil {
		log.Printf("Failed to report result for job %d: %v", jobID, err)
		return
	}
	defer resp.Body.Close()
}

func initRedis() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		// Fallback for local dev
		redisURL = "redis://localhost:6379/0"
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Invalid Redis URL: %v", err)
	}
	rdb = redis.NewClient(opt)

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Printf("Warning: Failed to ping Redis at startup: %v", err)
	}
}

func getPendingJobs() ([]common.FfmpegJob, error) {
	resp, err := httpClient.Get(apiBaseURL + "/ffmpeg-jobs/pending")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var jobs []common.FfmpegJob
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	var filteredJobs []common.FfmpegJob
	if len(jobs) > 0 {
		log.Println("Start filter jobs")
		for _, job := range jobs {
			log.Println("Job's endpoint is", job.Metadata.Endpoint)
			if strings.Contains(job.Metadata.Endpoint, os.Getenv("ZONE")) &&
				strings.Contains(job.Metadata.Endpoint, os.Getenv("PROVIDER")) &&
				isAllowedEndpoint(job.Metadata.Endpoint) {
				filteredJobs = append(filteredJobs, job)
			} else {
				totalCount := 0
				time := time.Now()
				updateJobStatus(job.ID, "PENDING", &time, "Endpoint check failed: zone/provider/private-endpoint policy mismatch", &totalCount)
				log.Println("Job endpoint check failed, skip")
			}
		}
		return filteredJobs, nil
	}
	log.Println("No jobs found,Waiting......")
	return filteredJobs, nil
}

func isAllowedEndpoint(endpoint string) bool {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u.Hostname() == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	privateOK := false
	if host == "localhost" {
		privateOK = true
	}
	if ip := net.ParseIP(host); ip != nil {
		privateOK = ip.IsPrivate() || ip.IsLoopback()
	}
	if !privateOK {
		for _, k := range internalEndpointKeywords {
			if strings.Contains(host, k) {
				privateOK = true
				break
			}
		}
	}
	if requirePrivateEndpoint && !privateOK {
		return false
	}
	if providerGuard == "aliyun" || providerGuard == "aliyuncs" || providerGuard == "oss" {
		return strings.Contains(host, "aliyuncs.com") && strings.Contains(host, "internal")
	}
	if providerGuard == "ivolces" || providerGuard == "volcengine" || providerGuard == "volc" || providerGuard == "tos" {
		return strings.Contains(host, "ivolces.com") && strings.Contains(host, "tos-s3-")
	}
	return true
}

func updateJobStatus(jobID int64, status string, lastScanTime *time.Time, msg string, totalCount *int) error {
	req := common.UpdateJobStatusRequest{
		Status:        status,
		LastScanTime:  lastScanTime,
		ResultMessage: msg,
	}
	if totalCount != nil {
		req.TotalCount = totalCount
	}

	data, _ := json.Marshal(req)

	reqObj, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/ffmpeg-jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
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

type FilePair struct {
	Video     string
	Audio     string
	VideoSize int64
	AudioSize int64
}

func processJob(job common.FfmpegJob) {
	startTime := time.Now()
	log.Printf("Processing Job %d: %s (Incremental: %v)", job.ID, job.S3Prefix, job.IsIncremental)

	if err := updateJobStatus(job.ID, "RUNNING", nil, "", nil); err != nil {
		if strings.Contains(err.Error(), "status 404") {
			cleanUpDedup(job.ID)
		}
		log.Printf("Failed to set RUNNING for job %d: %v", job.ID, err)
		return
	}

	s3Client, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, job.Metadata.SKEncrypted)
	if err != nil {
		log.Printf("Failed to init S3 for job %d: %v", job.ID, err)
		updateJobStatus(job.ID, "FAILED", nil, fmt.Sprintf("Init S3 failed: %v", err), nil)
		return
	}

	bucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	prefix := job.S3Prefix

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	pairs := make(map[string]*FilePair)
	pages := 0
	count := 0

	for paginator.HasMorePages() {
		pages++
		PagesScanned.Inc()
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("List failed for job %d: %v", job.ID, err)
			updateJobStatus(job.ID, "FAILED", nil, err.Error(), nil)
			return
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			name := filepath.Base(key)
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			if strings.Contains(name, "_video.") {
				id := strings.Split(name, "_video.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Video = key
				pairs[id].VideoSize = size
			} else if strings.Contains(name, "_audio.") {
				id := strings.Split(name, "_audio.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Audio = key
				pairs[id].AudioSize = size
			}
		}

		// Identify complete pairs (candidates)
		var candidates []struct {
			id     string
			pair   *FilePair
			taskID int64
		}

		for id, pair := range pairs {
			if pair.Video != "" && pair.Audio != "" {
				taskID := generateTaskID(filepath.Base(pair.Video)) // Deterministic Task ID based on Video Name
				candidates = append(candidates, struct {
					id     string
					pair   *FilePair
					taskID int64
				}{id, pair, taskID})
			}
		}

		if len(candidates) == 0 {
			continue
		}

		// Check for permanent failures before deduping
		var checkPermFailureIDs []interface{}
		for _, c := range candidates {
			checkPermFailureIDs = append(checkPermFailureIDs, c.taskID)
		}

		// If checkPermFailureIDs is empty, skip (though len check above prevents this)
		var permFailed []bool
		if len(checkPermFailureIDs) > 0 {
			// SIsMember does not support variadic args in older go-redis versions or depending on signature?
			// Actually SMISMEMBER is what we want if available, or just loop.
			// go-redis v9 has SMIsMember.

			res, err := rdb.SMIsMember(context.Background(), permFailureKey, checkPermFailureIDs...).Result()
			if err != nil {
				log.Printf("Failed to check for permanent failures: %v", err)
				// Assume none failed on error to be safe, or skip?
				// Better to assume false and let dedup handle or retry logic catch it later.
				permFailed = make([]bool, len(checkPermFailureIDs))
			} else {
				permFailed = res
			}
		}

		// Dedup
		pipe := rdb.Pipeline()
		dedupKey := fmt.Sprintf("%s%d", dedupPrefix, job.ID)
		var dedupCandidates []struct {
			id     string
			pair   *FilePair
			taskID int64
		}

		for i, c := range candidates {
			if i < len(permFailed) && permFailed[i] {
				// Skip permanently failed tasks
				continue
			}
			pipe.SAdd(context.Background(), dedupKey, c.taskID)
			dedupCandidates = append(dedupCandidates, c)
		}

		if len(dedupCandidates) == 0 {
			// Clear pairs that were filtered out or processed
			for _, c := range candidates {
				delete(pairs, c.id)
			}
			continue
		}

		cmders, err := pipe.Exec(context.Background())
		var batch [][]byte

		if err != nil {
			log.Printf("Dedup pipeline failed for job %d: %v", job.ID, err)
		} else {
			for i, cmder := range cmders {
				if cmd, ok := cmder.(*redis.IntCmd); ok {
					if cmd.Val() > 0 {
						// New task (SAdd returned 1)
						c := dedupCandidates[i]
						task := common.FfmpegTask{
							ID:        c.taskID,
							JobID:     job.ID,
							VideoKey:  c.pair.Video,
							AudioKey:  c.pair.Audio,
							VideoSize: c.pair.VideoSize,
							AudioSize: c.pair.AudioSize,
							Status:    "PENDING",
						}

						data, _ := json.Marshal(task)
						batch = append(batch, data)

						TasksDiscovered.Inc()
					}
				}
			}
		}

		// Always cleanup completed pairs from this page's processing
		for _, c := range candidates {
			delete(pairs, c.id)
		}

		if len(batch) > 0 {
			// Push batch to Redis
			// go-redis RPush accepts values...
			// We need []interface{}
			var interfaceBatch []interface{}
			for _, b := range batch {
				interfaceBatch = append(interfaceBatch, b)
			}

			var errPush error
			for i := 0; i < 3; i++ {
				errPush = rdb.RPush(context.Background(), taskQueue, interfaceBatch...).Err()
				if errPush == nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			if errPush != nil {
				log.Printf("Failed to push batch for job %d: %v", job.ID, errPush)
			}
			count += len(batch)
			if err := updateJobStatus(job.ID, "RUNNING", nil, "", &count); err != nil {
				if strings.Contains(err.Error(), "status 404") {
					cleanUpDedup(job.ID)
					return
				}
				log.Printf("Warning: failed to update status for job %d: %v", job.ID, err)
			}
		}
	}

	resultMsg := fmt.Sprintf("Scanned %d pages. Tasks Discovered: %d", pages, count)
	log.Printf("Job %d scan completed. %s", job.ID, resultMsg)

	var errFinal error
	if job.IsIncremental {
		// Keep running, update scan time
		errFinal = updateJobStatus(job.ID, "RUNNING", &startTime, resultMsg, nil)
	} else {
		errFinal = updateJobStatus(job.ID, "COMPLETED", &startTime, resultMsg, nil)
	}

	if errFinal != nil {
		if strings.Contains(errFinal.Error(), "status 404") {
			cleanUpDedup(job.ID)
		}
		log.Printf("Final status update failed for job %d: %v", job.ID, errFinal)
	}
}

func createS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}

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

func getBucketFromEndpoint(endpoint string) string {
	if strings.HasPrefix(endpoint, "s3://") {
		host := strings.TrimPrefix(endpoint, "s3://")
		parts := strings.Split(host, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return strings.Trim(u.Path, "/")
}

func generateTaskID(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	// Ensure positive ID
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func cleanUpDedup(jobID int64) {
	key := fmt.Sprintf("%s%d", dedupPrefix, jobID)
	if err := rdb.Del(context.Background(), key).Err(); err != nil {
		log.Printf("Failed to cleanup dedup key %s: %v", key, err)
	} else {
		log.Printf("Cleaned up dedup key %s for deleted job %d", key, jobID)
	}
}
