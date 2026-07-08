package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	DefaultTempDir = "/dev/shm"
)

var (
	apiBaseURL     string
	workerID       string
	CurrentTempDir string
	maxThreads     int = 1
	rdb            *redis.Client
	jobCache       sync.Map // JobID -> *common.FfmpegJob
	jobCacheExpiry sync.Map // JobID -> time.Time
	taskQueue      string
	failedQueue    string
	httpClient     *http.Client

	reservedSpace int64
	reservedMux   sync.Mutex

	TasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ffmpeg_worker_tasks_processed_total",
		Help: "Total number of tasks processed",
	}, []string{"status"})

	ProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "ffmpeg_worker_processing_duration_seconds",
		Help: "Duration of task processing",
	})
)

func handleTaskFailure(t common.FfmpegTask, reason string) {
	log.Printf("Task %d failed: %s", t.ID, reason)

	t.RetryCount++
	t.Status = "FAILED"
	t.ErrorMessage = reason
	t.UpdatedAt = time.Now()

	// Push to failed queue
	failedTaskData, err := json.Marshal(t)
	if err != nil {
		log.Printf("CRITICAL: Failed to marshal failed task %d for requeue: %v", t.ID, err)
		// If we can't marshal it, we just drop it but still report failure.
		reportResultPatch(t.JobID, false, 0)
		TasksProcessed.WithLabelValues("failed").Inc()
		return
	}

	if err := rdb.RPush(context.Background(), failedQueue, failedTaskData).Err(); err != nil {
		log.Printf("CRITICAL: Failed to push task %d to failed queue: %v", t.ID, err)
		// If we can't push to redis, we also have to drop it.
		reportResultPatch(t.JobID, false, 0)
		TasksProcessed.WithLabelValues("failed").Inc()
		return
	}

	// We don't report the failure to the main API here anymore.
	// The retry manager in the scanner will decide if it's a permanent failure.
}

func main() {
	rand.Seed(time.Now().UnixNano())
	workerID = fmt.Sprintf("ffmpeg-worker-%d", rand.Intn(1000000))

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}
	initQueueConfig()

	initRedis()

	httpClient = &http.Client{
		Timeout: 20 * time.Second,
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9094")
		http.ListenAndServe(":9094", nil)
	}()

	tempDir := strings.TrimSpace(os.Getenv("FFMPEG_TEMP_DIR"))
	if tempDir != "" {
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			log.Printf("Failed to initialize FFMPEG_TEMP_DIR=%s: %v", tempDir, err)
		} else {
			CurrentTempDir = tempDir
		}
	}
	if CurrentTempDir == "" {
		CurrentTempDir = DefaultTempDir
	}
	if _, err := os.Stat(CurrentTempDir); os.IsNotExist(err) {
		CurrentTempDir = os.TempDir()
		log.Printf("%s not found, using %s", DefaultTempDir, CurrentTempDir)
	}

	if mtStr := os.Getenv("MAX_THREADS"); mtStr != "" {
		if mt, err := strconv.Atoi(mtStr); err == nil && mt > 0 {
			maxThreads = mt
		}
	}

	total, free, _ := getDiskStats(CurrentTempDir)
	log.Printf("FFmpeg Worker %s Started. TempDir: %s, Capacity: %d bytes, Free: %d bytes, MaxThreads: %d", workerID, CurrentTempDir, total, free, maxThreads)

	// Worker Loop with Concurrency
	sem := make(chan struct{}, maxThreads)

	for {
		// BLPOP from Redis
		res, err := rdb.BLPop(context.Background(), 5*time.Second, taskQueue).Result()
		if err != nil {
			if err != redis.Nil {
				log.Printf("Redis error: %v", err)
				time.Sleep(1 * time.Second)
			}
			continue
		}

		// res[1] is payload
		var task common.FfmpegTask
		if err := json.Unmarshal([]byte(res[1]), &task); err != nil {
			log.Printf("Failed to unmarshal task: %v", err)
			continue
		}

		needed := int64(task.VideoSize+task.AudioSize) * 2

		// Wait for space
		for {
			reservedMux.Lock()
			total, free, err := getDiskStats(CurrentTempDir)
			if err != nil {
				log.Printf("Failed to get disk stats: %v", err)
				reservedMux.Unlock()
				time.Sleep(5 * time.Second)
				continue
			}

			used := total - free
			// Check if (Used + Reserved + Needed) > 80% of Total
			if float64(used+uint64(reservedSpace)+uint64(needed)) > float64(total)*0.8 {
				reservedMux.Unlock()
				log.Printf("Disk usage high (Used: %d, Reserved: %d, Needed: %d, Total: %d). Waiting...", used, reservedSpace, needed, total)
				time.Sleep(5 * time.Second)
				continue
			}

			reservedSpace += needed
			reservedMux.Unlock()
			break
		}

		sem <- struct{}{}
		go func(t common.FfmpegTask, reservedBytes int64) {
			defer func() {
				<-sem
				reservedMux.Lock()
				reservedSpace -= reservedBytes
				if reservedSpace < 0 {
					reservedSpace = 0
				}
				reservedMux.Unlock()
			}()
			processTask(t)
		}(task, needed)
	}
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
}

func initQueueConfig() {
	queuePrefix := strings.TrimSpace(os.Getenv("FFMPEG_QUEUE_PREFIX"))
	if queuePrefix == "" {
		queuePrefix = "ffmpeg"
	}
	queuePrefix = strings.Trim(queuePrefix, ":")
	taskQueue = fmt.Sprintf("queue:%s:pending", queuePrefix)
	failedQueue = fmt.Sprintf("queue:%s:failed", queuePrefix)
	log.Printf("FFmpeg queue config initialized: prefix=%s task=%s failed=%s", queuePrefix, taskQueue, failedQueue)
}

func getJobInfo(jobID int64) (*common.FfmpegJob, error) {
	if val, ok := jobCache.Load(jobID); ok {
		if expiry, ok := jobCacheExpiry.Load(jobID); ok {
			if time.Now().Before(expiry.(time.Time)) {
				return val.(*common.FfmpegJob), nil
			}
		}
	}

	resp, err := http.Get(fmt.Sprintf("%s/ffmpeg-jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var job common.FfmpegJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	jobCache.Store(jobID, &job)
	jobCacheExpiry.Store(jobID, time.Now().Add(5*time.Minute))
	return &job, nil
}

func reportResultPatch(jobID int64, success bool, successBytes int64) {
	req := common.UpdateJobStatusRequest{}
	if success {
		req.IncSuccess = 1
		if successBytes > 0 {
			req.IncSuccessBytes = successBytes
		}
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

func processTask(t common.FfmpegTask) {
	start := time.Now()
	log.Printf("Processing Task %d (Job %d), Retry: %d", t.ID, t.JobID, t.RetryCount)

	job, err := getJobInfo(t.JobID)
	if err != nil {
		handleTaskFailure(t, fmt.Sprintf("Failed to get job info: %v", err))
		return
	}

	s3Client, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, job.Metadata.SKEncrypted)
	if err != nil {
		handleTaskFailure(t, fmt.Sprintf("S3 Init Failed: %v", err))
		return
	}

	bucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	uploadPrefix := job.S3UploadPrefix
	if uploadPrefix == "" {
		uploadPrefix = job.S3Prefix
	}

	videoName := filepath.Base(t.VideoKey)
	id := strings.Split(videoName, "_video.")[0]

	outputKey := strings.TrimRight(uploadPrefix, "/") + "/" + id + ".mp4"
	if strings.HasPrefix(outputKey, "/") {
		outputKey = strings.TrimPrefix(outputKey, "/")
	}

	headOut, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(outputKey),
	})
	if err == nil {
		log.Printf("Output %s already exists, skipping.", outputKey)
		existingSize := int64(0)
		if headOut.ContentLength != nil {
			existingSize = *headOut.ContentLength
		}
		reportResultPatch(t.JobID, true, existingSize)
		TasksProcessed.WithLabelValues("skipped").Inc()
		return
	}

	workDir := CurrentTempDir
	localVideo := filepath.Join(workDir, fmt.Sprintf("%d_%s", t.ID, filepath.Base(t.VideoKey)))
	localAudio := filepath.Join(workDir, fmt.Sprintf("%d_%s", t.ID, filepath.Base(t.AudioKey)))
	localOutput := filepath.Join(workDir, fmt.Sprintf("%d_%s.mp4", t.ID, id))

	defer os.Remove(localVideo)
	defer os.Remove(localAudio)
	defer os.Remove(localOutput)

	if err := downloadFile(s3Client, bucket, t.VideoKey, localVideo); err != nil {
		handleTaskFailure(t, fmt.Sprintf("Download Video Failed: %v", err))
		return
	}
	if err := downloadFile(s3Client, bucket, t.AudioKey, localAudio); err != nil {
		handleTaskFailure(t, fmt.Sprintf("Download Audio Failed: %v", err))
		return
	}

	cmd := exec.Command("ffmpeg", "-y", "-i", localVideo, "-i", localAudio, "-c", "copy", "-map", "0:v:0", "-map", "1:a:0", localOutput)
	if output, err := cmd.CombinedOutput(); err != nil {
		handleTaskFailure(t, fmt.Sprintf("FFmpeg failed: %s", string(output)))
		return
	}

	f, err := os.Open(localOutput)
	if err != nil {
		handleTaskFailure(t, fmt.Sprintf("Open Output Failed: %v", err))
		return
	}
	defer f.Close()

	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(outputKey),
		Body:   f,
	})
	if err != nil {
		handleTaskFailure(t, fmt.Sprintf("Upload Failed: %v", err))
		return
	}

	outputInfo, statErr := os.Stat(localOutput)
	if statErr != nil {
		log.Printf("Stat output failed for task %d: %v", t.ID, statErr)
		reportResultPatch(t.JobID, true, 0)
	} else {
		reportResultPatch(t.JobID, true, outputInfo.Size())
	}
	TasksProcessed.WithLabelValues("success").Inc()
	ProcessingDuration.Observe(time.Since(start).Seconds())
	log.Printf("Task %d completed", t.ID)
}

func downloadFile(client *s3.Client, bucket, key, dest string) error {
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	return err
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



func getDiskStats(path string) (uint64, uint64, error) {

	var stat syscall.Statfs_t

	if err := syscall.Statfs(path, &stat); err != nil {

		return 0, 0, err

	}

	total := uint64(stat.Blocks) * uint64(stat.Bsize)

	free := uint64(stat.Bavail) * uint64(stat.Bsize)

	return total, free, nil

}
