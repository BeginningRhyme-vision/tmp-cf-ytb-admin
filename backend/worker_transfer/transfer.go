package main

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	ID            int64  `json:"id"`
	JobID         int64  `json:"job_id"`
	Src           string `json:"src"`
	Size          int64  `json:"size"`
	Status        string `json:"status"`
	RetryCount    int    `json:"retry_count"`
	LastRetryTime string `json:"last_retry_time"`
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
	jobCache                 sync.Map // JobID -> cachedJob
	httpClient               *http.Client
	transferClient           *http.Client
	transferTransport        *http.Transport
	workerCount              int
	taskBufferSize           int
	partConcurrency          int
	multipartThreshold       int64
	minPartSize              int64
	maxRetryCount            int
	retryBaseDelaySecs       int
	retryScannerIntervalSecs int
	minTimeoutSecs           int
	maxTimeoutSecs           int
	minThroughputMBPS        int
	chunkReadTimeoutSecs     int

	s3Clients sync.Map // Endpoint -> *s3.Client (Cache for Destinations)
	srcClient *s3.Client

	statsBuffer = make(map[int64]*JobStatsDelta)
	statsMutex  sync.Mutex

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	shutdownWg     sync.WaitGroup

	// Adaptive Concurrency Control
	adaptiveConcurrencyEnabled         bool
	minWorkers                         int
	maxWorkers                         int
	concurrencyIncreaseStep            int
	concurrencyDecreaseFactor          float64
	errorRateThreshold                 float64
	avgLatencyThresholdMS              int
	connPoolUtilizationThreshold       float64
	concurrencyAdjustIntervalSecs      int
	concurrencyCooldownSecs            int
	maxLatencySamples                  int

	effectiveWorkerCount       int32
	effectivePartConcurrency   int32
	activeWorkerCount          int32

	// Performance metrics for adaptive control
	recentLatencies         *list.List
	recentLatenciesMutex    sync.Mutex
	recentErrors            int32
	recentTotal             int32
	currentConnPoolUtilization float64

	// Sliding window metrics
	windowErrors            int32
	windowTotal             int32
	windowMutex             sync.Mutex

	// Cooldown tracking
	lastDecreaseTime        time.Time
	lastDecreaseTimeMutex   sync.Mutex

	// Log throttling - limit slot status logs to avoid spamming
	lastSlotLogTime         time.Time
	slotLogThrottleSecs     int
	slotLogMutex            sync.Mutex

	// Large file concurrency control
	largeFileThresholdBytes  int64
	maxLargeFiles            int
	maxRetryLargeFiles       int
	activeLargeFileCount     int32
	activeSmallFileCount     int32
	activeRetryLargeFileCount int32

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

	TaskRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "transfer_task_retries_total",
		Help: "Total number of task retries",
	})

	RetryDelayHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "transfer_task_retry_delay_seconds",
		Help:    "Distribution of retry delays between attempts",
		Buckets: prometheus.ExponentialBuckets(60, 2, 8),
	})

	RetrySuccessCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transfer_task_retry_success_total",
		Help: "Total successful retries by retry attempt number",
	}, []string{"retry_count"})

	RetryFailedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transfer_task_retry_failed_total",
		Help: "Total failed retries by retry attempt number",
	}, []string{"retry_count"})

	ConnPoolActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "transfer_conn_pool_active",
		Help: "Number of active connections in the connection pool",
	}, []string{"pool"})

	ConnPoolIdleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "transfer_conn_pool_idle",
		Help: "Number of idle connections in the connection pool",
	}, []string{"pool"})

	ConnPoolWaitCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transfer_conn_pool_wait_total",
		Help: "Total number of requests waiting for a connection",
	}, []string{"pool"})

	ConnPoolUsageGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "transfer_conn_pool_usage",
		Help: "Connection pool usage percentage (0-1)",
	}, []string{"pool"})
)

const (
	WorkerID                        = "go-transfer-1"
	DefaultConcurrentWorkers        = 128
	DefaultTaskBufferSize           = 256
	DefaultPartConcurrency          = 16
	DefaultMultipartThresholdMB     = 16
	DefaultMinPartSizeMB            = 8
	DefaultMaxConnections           = 2048
	DefaultMaxRetryCount            = 3
	DefaultRetryBaseDelaySecs       = 60
	DefaultRetryScannerIntervalSecs = 30
	DefaultMinTimeoutSecs           = 30
	DefaultMaxTimeoutSecs           = 10800
	DefaultMinThroughputMBPS        = 2
	DefaultChunkReadTimeoutSecs     = 30

	DefaultAdaptiveConcurrencyEnabled     = true
	DefaultMinWorkers                     = 8
	DefaultConcurrencyIncreaseStep        = 1
	DefaultConcurrencyDecreaseFactor      = 0.5
	DefaultErrorRateThreshold             = 0.05
	DefaultAvgLatencyThresholdMS          = 2000
	DefaultConnPoolUtilizationThreshold   = 0.8
	DefaultConcurrencyAdjustIntervalSecs  = 10
	DefaultConcurrencyCooldownSecs        = 30
	DefaultMaxLatencySamples              = 500

	DefaultLargeFileThresholdMB           = 1000
	DefaultMaxLargeFiles                  = 4
	DefaultMaxRetryLargeFiles             = 4
)

func runTransfer() {
	loadConfig()

	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Received shutdown signal, stopping gracefully...")
		shutdownCancel()
	}()

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
	maxConnections := getEnvInt("TRANSFER_MAX_CONNECTIONS", DefaultMaxConnections)
	maxRetryCount = getEnvInt("TRANSFER_MAX_RETRY_COUNT", DefaultMaxRetryCount)
	retryBaseDelaySecs = getEnvInt("TRANSFER_RETRY_BASE_DELAY_SECS", DefaultRetryBaseDelaySecs)
	retryScannerIntervalSecs = getEnvInt("TRANSFER_RETRY_SCANNER_INTERVAL_SECS", DefaultRetryScannerIntervalSecs)
	minTimeoutSecs = getEnvInt("TRANSFER_MIN_TIMEOUT_SECS", DefaultMinTimeoutSecs)
	maxTimeoutSecs = getEnvInt("TRANSFER_MAX_TIMEOUT_SECS", DefaultMaxTimeoutSecs)
	minThroughputMBPS = getEnvInt("TRANSFER_MIN_THROUGHPUT_MBPS", DefaultMinThroughputMBPS)
	chunkReadTimeoutSecs = getEnvInt("TRANSFER_CHUNK_READ_TIMEOUT_SECS", DefaultChunkReadTimeoutSecs)

	// Adaptive Concurrency Control Configuration
	adaptiveConcurrencyEnabled = getEnvBool("TRANSFER_ADAPTIVE_CONCURRENCY_ENABLED", DefaultAdaptiveConcurrencyEnabled)
	minWorkers = getEnvInt("TRANSFER_MIN_WORKERS", DefaultMinWorkers)
	maxWorkers = workerCount
	concurrencyIncreaseStep = getEnvInt("TRANSFER_CONCURRENCY_INCREASE_STEP", DefaultConcurrencyIncreaseStep)
	concurrencyDecreaseFactor = getEnvFloat("TRANSFER_CONCURRENCY_DECREASE_FACTOR", DefaultConcurrencyDecreaseFactor)
	errorRateThreshold = getEnvFloat("TRANSFER_ERROR_RATE_THRESHOLD", DefaultErrorRateThreshold)
	avgLatencyThresholdMS = getEnvInt("TRANSFER_AVG_LATENCY_THRESHOLD_MS", DefaultAvgLatencyThresholdMS)
	connPoolUtilizationThreshold = getEnvFloat("TRANSFER_CONN_POOL_UTILIZATION_THRESHOLD", DefaultConnPoolUtilizationThreshold)
	concurrencyAdjustIntervalSecs = getEnvInt("TRANSFER_CONCURRENCY_ADJUST_INTERVAL_SECS", DefaultConcurrencyAdjustIntervalSecs)
	concurrencyCooldownSecs = getEnvInt("TRANSFER_CONCURRENCY_COOLDOWN_SECS", DefaultConcurrencyCooldownSecs)
	maxLatencySamples = getEnvInt("TRANSFER_MAX_LATENCY_SAMPLES", DefaultMaxLatencySamples)
	slotLogThrottleSecs = getEnvInt("TRANSFER_SLOT_LOG_THROTTLE_SECS", 15)

	largeFileThresholdBytes = int64(getEnvInt("TRANSFER_LARGE_FILE_THRESHOLD_MB", DefaultLargeFileThresholdMB)) * 1024 * 1024
	maxLargeFiles = getEnvInt("TRANSFER_MAX_LARGE_FILES", DefaultMaxLargeFiles)
	maxRetryLargeFiles = getEnvInt("TRANSFER_MAX_RETRY_LARGE_FILES", DefaultMaxRetryLargeFiles)

	if maxLargeFiles < 1 {
		maxLargeFiles = 1
	}
	if maxRetryLargeFiles < 1 {
		maxRetryLargeFiles = 1
	}

	if minWorkers > maxWorkers {
		minWorkers = maxWorkers
	}

	atomic.StoreInt32(&effectiveWorkerCount, int32(maxWorkers))
	atomic.StoreInt32(&effectivePartConcurrency, int32(partConcurrency))
	recentLatencies = list.New()
	lastDecreaseTime = time.Unix(0, 0)

	httpClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     60 * time.Second,
			ForceAttemptHTTP2:   true,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	transferTransport = &http.Transport{
		MaxIdleConns:        maxConnections,
		MaxIdleConnsPerHost: maxConnections / 2,
		MaxConnsPerHost:     maxConnections,
		IdleConnTimeout:     300 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	transferClient = &http.Client{
		Transport: transferTransport,
		Timeout:   time.Duration(maxTimeoutSecs) * time.Second,
	}

	initSourceClient()
	initStatsFlusher()
	initConnPoolMetrics()

	log.Println("Transfer Worker Started")

	taskChan := make(chan TransferTask, taskBufferSize)

	// Start Fetcher
	go func() {
		shutdownWg.Add(1)
		defer shutdownWg.Done()

		for {
			select {
			case <-shutdownCtx.Done():
				log.Println("Fetcher stopping...")
				close(taskChan)
				return
			default:
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
					select {
					case taskChan <- t:
					case <-shutdownCtx.Done():
						log.Println("Fetcher stopping mid-task...")
						close(taskChan)
						return
					}
				}
			}
		}
	}()

	// Start Adaptive Concurrency Controller
	if adaptiveConcurrencyEnabled {
		go startConcurrencyController()
		log.Printf("Adaptive concurrency control enabled: min=%d, max=%d, interval=%ds",
			minWorkers, maxWorkers, concurrencyAdjustIntervalSecs)
	}

	// Start Workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskChan {
				if !acquireWorkerSlot() {
					time.Sleep(100 * time.Millisecond)
					taskChan <- t
					continue
				}
				processTask(t)
				releaseWorkerSlot()
			}
		}()
	}

	// Start Retry Scanner
	go startRetryScanner()

	wg.Wait()
	shutdownWg.Wait()

	log.Println("Transfer Worker stopped gracefully")
}

func calculateDynamicTimeout(size int64, retryCount int) time.Duration {
	minTimeout := time.Duration(minTimeoutSecs) * time.Second
	maxTimeout := time.Duration(maxTimeoutSecs) * time.Second

	if size <= 0 {
		return minTimeout
	}

	if minThroughputMBPS <= 0 {
		return maxTimeout
	}

	calculated := time.Duration(size/(int64(minThroughputMBPS)*1024*1024)) * time.Second
	
	if retryCount > 0 {
		retryMultiplier := float64(int64(1) << uint(retryCount))
		calculated = time.Duration(float64(calculated) * retryMultiplier)
	}

	if calculated < minTimeout {
		return minTimeout
	}
	if calculated > maxTimeout {
		return maxTimeout
	}
	return calculated
}

func calculatePartTimeout(partSize int64) time.Duration {
	return calculateDynamicTimeout(partSize, 0)
}

func acquireWorkerSlot() bool {
	if !adaptiveConcurrencyEnabled {
		return true
	}
	active := atomic.AddInt32(&activeWorkerCount, 1)
	effective := atomic.LoadInt32(&effectiveWorkerCount)
	defer func() {
		if !isWorkerSlotAvailable(active) {
			atomic.AddInt32(&activeWorkerCount, -1)
			log.Printf("[Adaptive] Worker slot REJECTED: active=%d, effective=%d (capped at max=%d)", active, effective, maxWorkers)
		}
	}()
	if active <= effective {
		log.Printf("[Adaptive] Worker slot ACQUIRED: active=%d, effective=%d, max=%d", active, effective, maxWorkers)
		return true
	}
	return false
}

func isWorkerSlotAvailable(active int32) bool {
	effective := atomic.LoadInt32(&effectiveWorkerCount)
	return active <= effective
}

func releaseWorkerSlot() {
	if adaptiveConcurrencyEnabled {
		active := atomic.AddInt32(&activeWorkerCount, -1)
		log.Printf("[Adaptive] Worker slot RELEASED: active=%d, effective=%d", active, atomic.LoadInt32(&effectiveWorkerCount))
	}
}

func acquirePartSlot(sem chan struct{}) bool {
	if !adaptiveConcurrencyEnabled {
		sem <- struct{}{}
		return true
	}
	effective := atomic.LoadInt32(&effectivePartConcurrency)
	if effective <= 0 {
		if shouldLogSlotStatus() {
			log.Printf("[Adaptive] Part slot REJECTED: effective=%d (disabled)", effective)
		}
		return false
	}
	select {
	case sem <- struct{}{}:
		if shouldLogSlotStatus() {
			log.Printf("[Adaptive] Part slot ACQUIRED: effective=%d", effective)
		}
		return true
	default:
		select {
		case sem <- struct{}{}:
			if shouldLogSlotStatus() {
				log.Printf("[Adaptive] Part slot ACQUIRED (delayed): effective=%d", effective)
			}
			return true
		default:
			if shouldLogSlotStatus() {
				log.Printf("[Adaptive] Part slot REJECTED: semaphore full, effective=%d", effective)
			}
			return false
		}
	}
}

func shouldLogSlotStatus() bool {
	slotLogMutex.Lock()
	defer slotLogMutex.Unlock()
	now := time.Now()
	if now.Sub(lastSlotLogTime).Seconds() >= float64(slotLogThrottleSecs) {
		lastSlotLogTime = now
		return true
	}
	return false
}

func recordTransferLatency(latencyMs int64) {
	if !adaptiveConcurrencyEnabled {
		return
	}
	recentLatenciesMutex.Lock()
	defer recentLatenciesMutex.Unlock()

	recentLatencies.PushBack(latencyMs)
	for recentLatencies.Len() > maxLatencySamples {
		recentLatencies.Remove(recentLatencies.Front())
	}
	log.Printf("[Adaptive] Latency recorded: %dms, total samples=%d (max=%d)", latencyMs, recentLatencies.Len(), maxLatencySamples)
}

func recordTransferResult(success bool) {
	if !adaptiveConcurrencyEnabled {
		return
	}

	total := atomic.AddInt32(&recentTotal, 1)
	var errors int32
	if !success {
		errors = atomic.AddInt32(&recentErrors, 1)
	}

	windowMutex.Lock()
	atomic.AddInt32(&windowTotal, 1)
	if !success {
		atomic.AddInt32(&windowErrors, 1)
	}
	windowMutex.Unlock()

	log.Printf("[Adaptive] Transfer result recorded: success=%v, total=%d, errors=%d, errorRate=%.2f%%",
		success, total, errors, float64(errors)/float64(total)*100)
}

func getErrorRate() float64 {
	total := atomic.LoadInt32(&windowTotal)
	if total == 0 {
		return 0
	}
	errors := atomic.LoadInt32(&windowErrors)
	return float64(errors) / float64(total)
}

func getAvgLatencyMs() float64 {
	recentLatenciesMutex.Lock()
	defer recentLatenciesMutex.Unlock()

	if recentLatencies.Len() == 0 {
		return 0
	}

	var sum int64 = 0
	for e := recentLatencies.Front(); e != nil; e = e.Next() {
		sum += e.Value.(int64)
	}
	return float64(sum) / float64(recentLatencies.Len())
}

func getConnPoolUtilization() float64 {
	return currentConnPoolUtilization
}

func startConcurrencyController() {
	shutdownWg.Add(1)
	defer shutdownWg.Done()

	ticker := time.NewTicker(time.Duration(concurrencyAdjustIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCtx.Done():
			log.Println("Concurrency controller stopping...")
			return
		case <-ticker.C:
			adjustConcurrency()
		}
	}
}

func adjustConcurrency() {
	errorRate := getErrorRate()
	avgLatency := getAvgLatencyMs()
	connPoolUtil := getConnPoolUtilization()
	currentEffective := atomic.LoadInt32(&effectiveWorkerCount)
	currentActive := atomic.LoadInt32(&activeWorkerCount)

	log.Printf("[Adaptive] ===== Concurrency Adjustment Check =====]")
	log.Printf("[Adaptive] Current state: effective=%d, active=%d, min=%d, max=%d",
		currentEffective, currentActive, minWorkers, maxWorkers)
	log.Printf("[Adaptive] Metrics: errorRate=%.2f%%, latency=%.0fms, connPool=%.2f",
		errorRate*100, avgLatency, connPoolUtil)
	log.Printf("[Adaptive] Thresholds: errorRate=%.2f%%, latency=%dms, connPool=%.2f",
		errorRateThreshold*100, avgLatencyThresholdMS, connPoolUtilizationThreshold)

	triggerReasons := []string{}
	shouldDecrease := false

	if errorRate > errorRateThreshold {
		reason := fmt.Sprintf("error_rate(%.2f%%>%.2f%%)", errorRate*100, errorRateThreshold*100)
		triggerReasons = append(triggerReasons, reason)
		log.Printf("[Adaptive] DECREASE TRIGGER: %s", reason)
		shouldDecrease = true
	}
	if avgLatency > float64(avgLatencyThresholdMS) {
		reason := fmt.Sprintf("latency(%.0fms>%dms)", avgLatency, avgLatencyThresholdMS)
		triggerReasons = append(triggerReasons, reason)
		log.Printf("[Adaptive] DECREASE TRIGGER: %s", reason)
		shouldDecrease = true
	}
	if connPoolUtil > connPoolUtilizationThreshold {
		reason := fmt.Sprintf("conn_pool(%.2f>%.2f)", connPoolUtil, connPoolUtilizationThreshold)
		triggerReasons = append(triggerReasons, reason)
		log.Printf("[Adaptive] DECREASE TRIGGER: %s", reason)
		shouldDecrease = true
	}

	lastDecreaseTimeMutex.Lock()
	timeSinceLastDecrease := time.Since(lastDecreaseTime).Seconds()
	lastDecreaseTimeMutex.Unlock()

	if shouldDecrease {
		if timeSinceLastDecrease < float64(concurrencyCooldownSecs) {
			log.Printf("[Adaptive] *** COOLDOWN PERIOD ACTIVE ***")
			log.Printf("[Adaptive] Last decrease was %.0fs ago, cooldown period is %ds",
				timeSinceLastDecrease, concurrencyCooldownSecs)
			log.Printf("[Adaptive] Skipping decrease, still in cooldown")
			log.Printf("[Adaptive] ===== End Concurrency Adjustment =====]")
			return
		}

		decreaseFactor := concurrencyDecreaseFactor
		if len(triggerReasons) >= 2 {
			decreaseFactor = 0.4
			log.Printf("[Adaptive] Multiple triggers (%d), using more aggressive decrease factor: %.2f",
				len(triggerReasons), decreaseFactor)
		}

		newEffective := int32(float64(currentEffective) * decreaseFactor)
		if newEffective < int32(minWorkers) {
			newEffective = int32(minWorkers)
		}

		if newEffective != currentEffective {
			lastDecreaseTimeMutex.Lock()
			lastDecreaseTime = time.Now()
			lastDecreaseTimeMutex.Unlock()

			log.Printf("[Adaptive] *** CONCURRENCY DECREASE TRIGGERED ***")
			log.Printf("[Adaptive] Trigger reasons: %s", strings.Join(triggerReasons, ", "))
			log.Printf("[Adaptive] Decreasing effective workers: %d -> %d (factor=%.2f)",
				currentEffective, newEffective, decreaseFactor)

			oldPartConcurrency := atomic.LoadInt32(&effectivePartConcurrency)
			newPartConcurrency := int32(float64(partConcurrency) * decreaseFactor)
			if newPartConcurrency < 1 {
				newPartConcurrency = 1
			}
			atomic.StoreInt32(&effectiveWorkerCount, newEffective)
			atomic.StoreInt32(&effectivePartConcurrency, newPartConcurrency)

			log.Printf("[Adaptive] Part concurrency adjusted: %d -> %d", oldPartConcurrency, newPartConcurrency)
			log.Printf("[Adaptive] Minimum workers boundary: %d", minWorkers)
			log.Printf("[Adaptive] Cooldown period started: %ds", concurrencyCooldownSecs)
		} else {
			log.Printf("[Adaptive] No change needed: already at minimum effective workers=%d", currentEffective)
			log.Printf("[Adaptive] Trigger conditions met but cannot decrease further (at min=%d)", minWorkers)
		}
	} else {
		if currentEffective < int32(maxWorkers) {
			increaseStep := concurrencyIncreaseStep
			if timeSinceLastDecrease < float64(concurrencyCooldownSecs) {
				log.Printf("[Adaptive] Still in cooldown period (%.0fs remaining), slower recovery",
					timeSinceLastDecrease)
				increaseStep = 1
			} else if currentEffective < int32(maxWorkers)*2/3 {
				increaseStep = concurrencyIncreaseStep * 2
				log.Printf("[Adaptive] Below 2/3 of max workers, accelerated recovery: step=%d", increaseStep)
			}

			newEffective := currentEffective + int32(increaseStep)
			if newEffective > int32(maxWorkers) {
				newEffective = int32(maxWorkers)
			}

			if newEffective != currentEffective {
				log.Printf("[Adaptive] *** CONCURRENCY INCREASE ***")
				log.Printf("[Adaptive] All conditions within thresholds, increasing effective workers: %d -> %d (step=%d)",
					currentEffective, newEffective, increaseStep)

				oldPartConcurrency := atomic.LoadInt32(&effectivePartConcurrency)
				newPartConcurrency := int32(float64(partConcurrency)*(float64(newEffective)/float64(maxWorkers)) + 0.5)
				if newPartConcurrency < 1 {
					newPartConcurrency = 1
				}
				atomic.StoreInt32(&effectiveWorkerCount, newEffective)
				atomic.StoreInt32(&effectivePartConcurrency, newPartConcurrency)

				log.Printf("[Adaptive] Part concurrency adjusted: %d -> %d", oldPartConcurrency, newPartConcurrency)
				log.Printf("[Adaptive] Max workers: %d", maxWorkers)
			} else {
				log.Printf("[Adaptive] No change needed: already at maximum effective workers=%d", currentEffective)
			}
		} else {
			log.Printf("[Adaptive] No change: at maximum effective workers=%d", currentEffective)
			log.Printf("[Adaptive] All metrics healthy: errorRate=%.2f%%, avgLatency=%.0fms, connPoolUtil=%.2f",
				errorRate*100, avgLatency, connPoolUtil)
		}
	}

	log.Printf("[Adaptive] ===== End Concurrency Adjustment =====]")

	windowMutex.Lock()
	windowTotal = 0
	windowErrors = 0
	windowMutex.Unlock()

	atomic.StoreInt32(&recentTotal, 0)
	atomic.StoreInt32(&recentErrors, 0)
	recentLatenciesMutex.Lock()
	recentLatencies.Init()
	recentLatenciesMutex.Unlock()
}

func validateTimeoutConfig(min, max, throughput int) bool {
	if min <= 0 || max <= 0 || throughput <= 0 {
		return false
	}
	if min > max {
		return false
	}
	return true
}

func initStatsFlusher() {
	go func() {
		shutdownWg.Add(1)
		defer shutdownWg.Done()

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-shutdownCtx.Done():
				log.Println("Stats flusher stopping...")
				return
			case <-ticker.C:
				flushStats()
			}
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

func initConnPoolMetrics() {
	go func() {
		shutdownWg.Add(1)
		defer shutdownWg.Done()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-shutdownCtx.Done():
				log.Println("ConnPool metrics collector stopping...")
				return
			case <-ticker.C:
				updateConnPoolMetrics()
			}
		}
	}()
}

func updateConnPoolMetrics() {
	if transferTransport == nil {
		return
	}

	t := reflect.ValueOf(transferTransport).Elem()

	active := getIntField(t, "numActive")
	idle := getIntField(t, "numIdle")
	waitCount := getIntField(t, "waitCount")

	ConnPoolActiveGauge.WithLabelValues("transfer").Set(float64(active))
	ConnPoolIdleGauge.WithLabelValues("transfer").Set(float64(idle))
	ConnPoolWaitCounter.WithLabelValues("transfer").Add(float64(waitCount))

	maxConns := transferTransport.MaxConnsPerHost
	if maxConns > 0 {
		usage := float64(active) / float64(maxConns)
		ConnPoolUsageGauge.WithLabelValues("transfer").Set(usage)
		currentConnPoolUtilization = usage
	}
}

func getIntField(v reflect.Value, name string) int {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return 0
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return int(f.Int())
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		return int(f.Uint())
	default:
		return 0
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

	if t.RetryCount >= maxRetryCount {
		log.Printf("Task %d has reached max retry count (%d), skipping", t.ID, maxRetryCount)
		return
	}

	job, err := getJobInfo(t.JobID)
	if err != nil {
		log.Printf("Failed to get job info for task %d: %v", t.ID, err)
		updateTaskStatusWithRetry(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		recordTransferResult(false)
		return
	}

	srcDir := strings.TrimSpace(job.SrcDir)
	dstDir := strings.TrimSpace(job.DstDir)

	log.Printf("Processing Task %d (Job %d): %s -> %s", t.ID, t.JobID, t.Src, dstDir)

	if t.Size >= largeFileThresholdBytes {
		isRetry := t.RetryCount > 0
		for {
			if atomic.LoadInt32(&activeSmallFileCount) == 0 {
				if isRetry {
					current := atomic.LoadInt32(&activeRetryLargeFileCount)
					if current < int32(maxRetryLargeFiles) {
						if atomic.CompareAndSwapInt32(&activeRetryLargeFileCount, current, current+1) {
							atomic.AddInt32(&activeLargeFileCount, 1)
							log.Printf("[LargeFile] Task %d (%d bytes, retry=%d) acquired retry slot in unlimited mode (%d/%d)",
								t.ID, t.Size, t.RetryCount, current+1, maxRetryLargeFiles)
							defer func() {
								atomic.AddInt32(&activeLargeFileCount, -1)
								atomic.AddInt32(&activeRetryLargeFileCount, -1)
							}()
							break
						}
					} else {
						log.Printf("[LargeFile] Task %d (%d bytes, retry=%d) waiting for retry slot in unlimited mode (%d/%d)",
							t.ID, t.Size, t.RetryCount, current, maxRetryLargeFiles)
					}
				} else {
					atomic.AddInt32(&activeLargeFileCount, 1)
					log.Printf("[LargeFile] Task %d (%d bytes) acquired slot in unlimited mode (first attempt)", t.ID, t.Size)
					defer atomic.AddInt32(&activeLargeFileCount, -1)
					break
				}
			} else {
				current := atomic.LoadInt32(&activeLargeFileCount)
				if current < int32(maxLargeFiles) {
					if atomic.CompareAndSwapInt32(&activeLargeFileCount, current, current+1) {
						if isRetry {
							atomic.AddInt32(&activeRetryLargeFileCount, 1)
							log.Printf("[LargeFile] Task %d (%d bytes, retry=%d) acquired large file slot (%d/%d), small files active",
								t.ID, t.Size, t.RetryCount, current+1, maxLargeFiles)
							defer func() {
								atomic.AddInt32(&activeLargeFileCount, -1)
								atomic.AddInt32(&activeRetryLargeFileCount, -1)
							}()
						} else {
							log.Printf("[LargeFile] Task %d (%d bytes) acquired large file slot (%d/%d), small files active",
								t.ID, t.Size, current+1, maxLargeFiles)
							defer atomic.AddInt32(&activeLargeFileCount, -1)
						}
						break
					}
				} else {
					log.Printf("[LargeFile] Task %d (%d bytes, retry=%d) waiting for large file slot (%d/%d), small files active",
						t.ID, t.Size, t.RetryCount, current, maxLargeFiles)
				}
			}
			time.Sleep(5 * time.Second)
		}
	} else {
		atomic.AddInt32(&activeSmallFileCount, 1)
		defer atomic.AddInt32(&activeSmallFileCount, -1)
	}

	// 1. Resolve Dst Client
	sk := job.Metadata.SKEncrypted
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}

	dstClient, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, sk)
	if err != nil {
		log.Printf("Dst client init failed for task %d: %v", t.ID, err)
		updateTaskStatusWithRetry(t, "FAILED", "Dst client init failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		recordTransferResult(false)
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
		headCtx, headCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer headCancel()
		head, err := srcClient.HeadObject(headCtx, &s3.HeadObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(srcKey),
		})
		if err != nil {
			log.Printf("HeadObject failed for %s/%s: %v", srcBucket, srcKey, err)
			updateTaskStatusWithRetry(t, "FAILED", "HeadObject failed: "+err.Error())
			updateJobStats(t.JobID, 0, 1)
			TasksTransferred.WithLabelValues("failed").Inc()
			recordTransferResult(false)
			return
		}
		size = *head.ContentLength
	}

	// 4. Construct Public/Virtual-Hosted URLs for Transfer Service (Matches r2s3.go logic)
	srcUrl, err := constructVirtualHostURL(cfg.Storage.Src.Endpoint, srcBucket, srcKey)
	if err != nil {
		log.Printf("Failed to construct Src URL for task %d: %v", t.ID, err)
		updateTaskStatusWithRetry(t, "FAILED", "Construct Src URL failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		recordTransferResult(false)
		return
	}

	dstBucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	log.Printf("Task %d: Transferring %d bytes to bucket '%s' key '%s'", t.ID, size, dstBucket, dstKey)

	taskTimeout := calculateDynamicTimeout(size, t.RetryCount)
	log.Printf("Task %d: Calculated dynamic timeout: %v (retry=%d)", t.ID, taskTimeout, t.RetryCount)

	taskCtx, taskCancel := context.WithTimeout(shutdownCtx, taskTimeout)
	defer taskCancel()

	err = transferFile(taskCtx, srcUrl, dstClient, dstBucket, dstKey, size, job.Metadata.Endpoint)
	if err != nil {
		log.Printf("Transfer failed for task %d: %v", t.ID, err)
		updateTaskStatusWithRetry(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		recordTransferResult(false)
	} else {
		log.Printf("Task %d completed successfully", t.ID)

		if job.DeleteSource {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer deleteCancel()
			_, err := srcClient.DeleteObject(deleteCtx, &s3.DeleteObjectInput{
				Bucket: aws.String(srcBucket),
				Key:    aws.String(srcKey),
			})
			if err != nil {
				log.Printf("Failed to delete source object %s for task %d: %v", srcKey, t.ID, err)
			} else {
				log.Printf("Deleted source object %s for task %d", srcKey, t.ID)
			}
		}

		updateTaskStatusWithRetry(t, "COMPLETED", "")
		updateJobStats(t.JobID, 1, 0)

		// Metrics
		duration := time.Since(start).Seconds()
		TransferDuration.Observe(duration)
		BytesTransferred.Add(float64(size))
		TasksTransferred.WithLabelValues("success").Inc()
		recordTransferResult(true)
		recordTransferLatency(int64(duration * 1000))
	}
}

func transferFile(ctx context.Context, srcURL string, dstClient *s3.Client, dstBucket, dstKey string, size int64, dstEndpoint string) error {
	dstUrl, err := constructVirtualHostURL(dstEndpoint, dstBucket, dstKey)
	if err != nil {
		return err
	}

	if size < multipartThreshold {
		_, err = callTransferService(ctx, srcURL, dstUrl, size, 0, "", -1)
		return err
	}

	createCtx, createCancel := context.WithTimeout(ctx, 60*time.Second)
	defer createCancel()
	createOut, err := dstClient.CreateMultipartUpload(createCtx, &s3.CreateMultipartUploadInput{
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
			if !acquirePartSlot(sem) {
				return
			}
			defer func() { <-sem }()

			partSizeBytes := e - s + 1
			partTimeout := calculatePartTimeout(partSizeBytes)
			partCtx, partCancel := context.WithTimeout(ctx, partTimeout)
			defer partCancel()

			etag, err := callTransferService(partCtx, srcURL, dstUrl, partSizeBytes, s, uploadID, int(pNum))
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
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer abortCancel()
		dstClient.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		})
		return err
	default:
	}

	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
	}

	completeCtx, completeCancel := context.WithTimeout(ctx, 60*time.Second)
	defer completeCancel()
	_, err = dstClient.CompleteMultipartUpload(completeCtx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	return err
}

func callTransferService(ctx context.Context, srcUrl, dstUrl string, size, offset int64, uploadID string, partNum int) (string, error) {
	payload := map[string]interface{}{
		"r2Key":      srcUrl,
		"s3Url":      dstUrl,
		"size":       size,
		"offset":     offset,
		"uploadId":   uploadID,
		"partNumber": partNum,
	}

	body, _ := json.Marshal(payload)

	var lastErr error
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, "POST", cfg.Storage.TransferServiceURL, bytes.NewBuffer(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := transferClient.Do(req)
		if err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			var res map[string]interface{}
			if err := json.Unmarshal(respBody, &res); err != nil {
				lastErr = fmt.Errorf("decode response failed: %w", err)
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				default:
					time.Sleep(time.Duration(i+1) * time.Second)
					continue
				}
			}
			if etag, ok := res["etag"].(string); ok {
				return etag, nil
			}
			lastErr = fmt.Errorf("etag missing in response")
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
		}
		lastErr = fmt.Errorf("status %d body %s", resp.StatusCode, string(respBody))
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			time.Sleep(time.Duration(i+1) * time.Second)
		}
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

	httpTransport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 256,
		MaxConnsPerHost:     512,
		IdleConnTimeout:     300 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		awsconfig.WithRegion("auto"),
		awsconfig.WithHTTPClient(&http.Client{
			Transport: httpTransport,
			Timeout:   time.Duration(maxTimeoutSecs) * time.Second,
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

func getEnvBool(key string, defaultValue bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	switch strings.ToLower(v) {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func getEnvFloat(key string, defaultValue float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return defaultValue
	}
	return f
}

func updateTaskStatusWithRetry(t TransferTask, status, msg string) {
	t.Status = status

	var newRetryCount int
	var newLastRetryTime string

	if status == "FAILED" {
		newRetryCount = t.RetryCount + 1
		newLastRetryTime = time.Now().Format(time.RFC3339)
		TaskRetries.Inc()
		RetryFailedCounter.WithLabelValues(strconv.Itoa(newRetryCount)).Inc()
	} else if status == "COMPLETED" {
		newRetryCount = 0
		newLastRetryTime = ""
		if t.RetryCount > 0 {
			RetrySuccessCounter.WithLabelValues(strconv.Itoa(t.RetryCount)).Inc()
		}
	} else {
		newRetryCount = t.RetryCount
		newLastRetryTime = t.LastRetryTime
	}

	t.RetryCount = newRetryCount
	t.LastRetryTime = newLastRetryTime

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

func startRetryScanner() {
	shutdownWg.Add(1)
	defer shutdownWg.Done()

	log.Printf("Retry Scanner started with interval %ds, max retry count %d, base delay %ds",
		retryScannerIntervalSecs, maxRetryCount, retryBaseDelaySecs)

	ticker := time.NewTicker(time.Duration(retryScannerIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCtx.Done():
			log.Println("Retry Scanner stopping...")
			return
		case <-ticker.C:
			failedTasks, err := getFailedTasksForRetry(maxRetryCount)
			if err != nil {
				log.Printf("Failed to get failed tasks for retry: %v", err)
				continue
			}

			if len(failedTasks) == 0 {
				continue
			}

			log.Printf("Found %d failed tasks for retry check", len(failedTasks))

			for _, task := range failedTasks {
				if !shouldRetryTask(task) {
					continue
				}

				log.Printf("Resetting task %d to PENDING (retry count: %d)", task.ID, task.RetryCount)
				if err := resetTaskToPending(task.ID); err != nil {
					log.Printf("Failed to reset task %d to PENDING: %v", task.ID, err)
				}
			}
		}
	}
}

func getFailedTasksForRetry(maxRetryCount int) ([]TransferTask, error) {
	payload := map[string]interface{}{
		"max_retry_count": maxRetryCount,
	}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/failed-for-retry", "application/json", bytes.NewBuffer(data))
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

func shouldRetryTask(task TransferTask) bool {
	if task.RetryCount >= maxRetryCount {
		return false
	}

	if task.LastRetryTime == "" {
		return true
	}

	lastRetryTime, err := time.Parse(time.RFC3339, task.LastRetryTime)
	if err != nil {
		return true
	}

	var retryDelay time.Duration
	if task.RetryCount < 30 {
		retryDelay = time.Duration(int64(retryBaseDelaySecs)*int64(1<<task.RetryCount)) * time.Second
	} else {
		retryDelay = time.Duration(int64(retryBaseDelaySecs)*1073741824) * time.Second
	}

	if time.Now().After(lastRetryTime.Add(retryDelay)) {
		RetryDelayHistogram.Observe(retryDelay.Seconds())
		return true
	}

	return false
}

func resetTaskToPending(taskID int64) error {
	payload := map[string]interface{}{
		"task_id": taskID,
		"status":  "PENDING",
	}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/reset", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
