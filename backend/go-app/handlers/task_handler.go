package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/metrics"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Global Buffer Manager
var (
	jobBuffers       = make(map[int64]chan models.YoutubeTask)
	txJobBuffers     = make(map[int64]chan models.TransferTask)
	ffmpegJobBuffers = make(map[int64]chan models.FfmpegTask)
	bufferMutex      sync.RWMutex
	fillingMap       sync.Map // prevent concurrent fills for same job

	// Stats Buffer for Postgres Sync
	statsBuffer     = make(map[int64]*JobDelta)
	statsMutex      sync.Mutex
	jobShardingMode sync.Map
)

type JobDelta struct {
	Pending int
	Running int
	Success int
	Failed  int
}

const (
	BufferSize       = 2000
	FetchBatchSize   = 100
	BufferLowWater   = 100 // Refill when below this
	TxFetchBatchSize = 1000
	LockExpiration   = 30 * time.Second
	DedupShards      = 256   // 去重 Hash 分成 256 片
	TaskBucketSize   = 50000 // 任务 ZSet 每 5 万个 ID 分一个桶
)

// 获取 job 模式
func isJobSharded(ctx context.Context, jobID int64) bool {
	// 1. 先查内存缓存
	if val, ok := jobShardingMode.Load(jobID); ok {
		return val.(bool)
	}

	// 2. 内存没有，去 Redis 探测旧 Key 是否存在
	oldKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	exists, _ := database.RDB.Exists(ctx, oldKey).Result()

	// 如果旧 Key 存在，说明是旧任务 (isSharded = false)
	// 如果旧 Key 不存在，默认为新任务 (isSharded = true)
	isSharded := (exists == 0)

	// 写入缓存
	jobShardingMode.Store(jobID, isSharded)
	return isSharded
}

// 计算去重 Key 的分片
func getDedupShardKey(jobID int64, src string) string {
	h := fnv32(src)
	shard := h % DedupShards
	return fmt.Sprintf("tx:job:%d:dedup:%d", jobID, shard)
}

// 计算任务 Bucket Key
func getTaskBucketKey(jobID int64, taskID int64) string {
	bucket := taskID / TaskBucketSize
	return fmt.Sprintf("tx:job:%d:tasks:%d", jobID, bucket)
}
func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// StartBufferService initializes the background pre-fetching service
func StartBufferService() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		monitorTicker := time.NewTicker(10 * time.Minute) // Check for stuck tasks every 10m
		for {
			select {
			case <-ticker.C:
				checkAndRefillBuffers()
				checkAndRefillTxBuffers()
				checkAndRefillFfmpegBuffers()
				flushStats()
			case <-monitorTicker.C:
				scanStuckYoutubeTasks()
			}
		}
	}()
}

func scanStuckYoutubeTasks() {
	ctx := context.Background()
	var jobs []models.YoutubeJob
	// Only scan running/pending jobs
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		jobKey := fmt.Sprintf("job:%d:tasks", job.ID)
		batchSize := 1000
		var offset int64 = 0

		for {
			ids, err := database.RDB.ZRange(ctx, jobKey, offset, offset+int64(batchSize)-1).Result()
			if err != nil || len(ids) == 0 {
				break
			}

			var keys []string
			for _, id := range ids {
				keys = append(keys, fmt.Sprintf("task:%s", id))
			}

			tasksJSON, err := database.RDB.MGet(ctx, keys...).Result()
			if err != nil {
				offset += int64(batchSize)
				continue
			}

			pipe := database.RDB.Pipeline()
			hasUpdates := false

			for _, val := range tasksJSON {
				if val == nil {
					continue
				}
				str, ok := val.(string)
				if !ok {
					continue
				}

				var t models.YoutubeTask
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					// Check if task is stuck (RUNNING or METADATA_PROCESSING) AND Created > 3 hours
					// METADATA_PROCESSING 是 metadata worker 使用的状态，避免与 download worker 的 RUNNING 混淆
					if (t.Status == "RUNNING" || t.Status == "METADATA_PROCESSING") && time.Since(t.CreatedAt) > 3*time.Hour {
						oldStatus := t.Status
						t.Status = "PENDING"
						t.WorkerID = ""
						t.ErrorMessage = "Reset by stuck monitor"
						t.UpdatedAt = time.Now()
						t.IsDownloadFail = false

						data, _ := json.Marshal(t)
						pipe.Set(ctx, fmt.Sprintf("task:%d", t.ID), data, 0)
						// 根据 Job 的 MachineName 推入对应的 metadata 队列
						queueName := getMetadataQueueName(t.JobID)
						pipe.RPush(ctx, queueName, t.ID)

						trackStatusChange(t.JobID, oldStatus, "PENDING")
						hasUpdates = true
					}
				}
			}

			if hasUpdates {
				pipe.Exec(ctx)
			}

			if len(ids) < batchSize {
				break
			}
			offset += int64(batchSize)
		}
	}
}

func flushStats() {
	statsMutex.Lock()
	if len(statsBuffer) == 0 {
		statsMutex.Unlock()
		return
	}

	// Copy and clear
	snapshot := make(map[int64]JobDelta)
	for k, v := range statsBuffer {
		snapshot[k] = *v
	}
	statsBuffer = make(map[int64]*JobDelta)
	statsMutex.Unlock()

	// Execute updates
	for jobID, delta := range snapshot {
		// Construct query
		// Using raw SQL for atomic updates
		// We also update status based on the *new* counts
		// Use GREATEST(0, ...) to prevent negative counts
		query := `
			UPDATE youtube_jobs SET 
				pending_count = GREATEST(0, pending_count + ?), 
				running_count = GREATEST(0, running_count + ?), 
				success_count = GREATEST(0, success_count + ?), 
				failed_count = GREATEST(0, failed_count + ?),
				status = CASE 
					WHEN status = 'PENDING' AND (GREATEST(0, running_count + ?)) > 0 THEN 'RUNNING'
					WHEN status = 'RUNNING' AND (GREATEST(0, pending_count + ?)) <= 0 AND (GREATEST(0, running_count + ?)) <= 0 AND success_count + failed_count + ? + ? > 0 THEN 'COMPLETED'
					ELSE status 
				END
			WHERE id = ?
		`
		database.DB.Exec(query,
			delta.Pending, delta.Running, delta.Success, delta.Failed, // For count updates
			delta.Running,                // For PENDING->RUNNING check
			delta.Pending, delta.Running, // For RUNNING->COMPLETED check
			delta.Success, delta.Failed,  // For total_count > 0 check
			jobID,
		)
	}
}

func trackStatusChange(jobID int64, oldStatus, newStatus string) {
	if oldStatus == newStatus {
		return
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	if _, ok := statsBuffer[jobID]; !ok {
		statsBuffer[jobID] = &JobDelta{}
	}
	d := statsBuffer[jobID]

	// Helper to map status to bucket
	getBucket := func(s string) string {
		switch s {
		case "PENDING":
			return "PENDING"
		case "COMPLETED":
			return "COMPLETED"
		case "FAILED":
			return "FAILED"
		default:
			return "RUNNING" // Everything else (METADATA_FETCHED, RUNNING, etc.) is running
		}
	}

	oldBucket := getBucket(oldStatus)
	newBucket := getBucket(newStatus)

	if oldBucket == newBucket {
		return
	}

	metrics.TaskStatusChangeTotal.WithLabelValues("youtube", newBucket).Inc()

	// Decrement old
	switch oldBucket {
	case "PENDING":
		d.Pending--
	case "RUNNING":
		d.Running--
	case "COMPLETED":
		d.Success--
	case "FAILED":
		d.Failed--
	}

	// Increment new
	switch newBucket {
	case "PENDING":
		d.Pending++
	case "RUNNING":
		d.Running++
	case "COMPLETED":
		d.Success++
	case "FAILED":
		d.Failed++
	}
}

func checkAndRefillBuffers() {
	var jobs []models.YoutubeJob
	// Find active jobs
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		jid := int64(job.ID)
		ensureBuffer(jid)

		bufferMutex.RLock()
		ch, exists := jobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			continue
		}

		// Check for stuck state: Pending > 0, Buffer Empty, Not Filling
		// Note: Offset reset is now manual via API endpoint, not automatic
		if job.PendingCount > 0 && len(ch) == 0 {
			// Buffer is empty but there are pending tasks, trigger refill if not already filling
			if _, filling := fillingMap.Load(jid); !filling {
				// Offset reset removed - use manual API endpoint instead
			}
		}

		if len(ch) < BufferLowWater {
			triggerRefill(jid)
		}
	}
}

func ensureBuffer(jobID int64) {
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	if _, ok := jobBuffers[jobID]; !ok {
		jobBuffers[jobID] = make(chan models.YoutubeTask, BufferSize)
	}
}

func triggerRefill(jobID int64) {
	// Trigger refill if not already filling
	if _, filling := fillingMap.Load(jobID); !filling {
		fillingMap.Store(jobID, true)
		go func(jid int64) {
			defer fillingMap.Delete(jid)
			fillJobBuffer(jid)
		}(jobID)
	}
}

func fillJobBuffer(jobID int64) {
	ctx := context.Background()
	lockKey := fmt.Sprintf("job:%d:lock", jobID)

	// Acquire Lock
	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()
	if err != nil || !ok {
		return // Failed to acquire lock
	}
	defer database.RDB.Del(ctx, lockKey)

	// Get Offset
	offsetKey := fmt.Sprintf("job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var offset int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}

	// Fetch batch from Redis
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)
	start := offset
	stop := offset + int64(FetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()
	if err != nil || len(ids) == 0 {
		return
	}

	// Fetch details
	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("task:%s", id))
	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		return
	}

	// Push to buffer
	bufferMutex.RLock()
	ch, exists := jobBuffers[jobID]
	bufferMutex.RUnlock()

	if !exists {
		return
	}

	count := 0
	processed := 0
	for _, item := range jsonList {
		processed++
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}

			// Non-blocking send or timeout?
			// Since we check len < LowWater, there should be space.
			// But to be safe, use select
			select {
			case ch <- task:
				count++
			default:
				// Buffer full, stop filling
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	// Update offset
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}
}

func BatchInsert(c *gin.Context) {
	var tasks []models.YoutubeTask
	if err := c.ShouldBindJSON(&tasks); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(tasks) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No tasks to insert"})
		return
	}

	ctx := context.Background()
	pipe := database.RDB.Pipeline()

	successCount := 0
	for _, task := range tasks {
		data, err := json.Marshal(task)
		if err != nil {
			log.Printf("WARNING: BatchInsert: Failed to marshal task %d for job %d: %v", task.ID, task.JobID, err)
			continue
		}

		taskKey := fmt.Sprintf("task:%d", task.ID)
		jobKey := fmt.Sprintf("job:%d:tasks", task.JobID)

		pipe.Set(ctx, taskKey, data, 0)
		pipe.ZAdd(ctx, jobKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})
		successCount++
	}

	if successCount == 0 {
		c.JSON(http.StatusOK, gin.H{"count": 0, "message": "No valid tasks to insert"})
		return
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("ERROR: BatchInsert: Failed to execute pipeline: %v. %d tasks may have been created but not added to sorted set", err, successCount)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save tasks: " + err.Error()})
		return
	}

	log.Printf("INFO: BatchInsert: Successfully inserted %d tasks", successCount)
	c.JSON(http.StatusOK, gin.H{"count": successCount})
}

func BatchUpdate(c *gin.Context) {
	type BatchUpdateRequest struct {
		Updates     []models.YoutubeTask `json:"updates"`
		MachineName string               `json:"machine_name"` // 可选：处理这些任务的 worker 的机器名
	}

	var req BatchUpdateRequest
	var updates []models.YoutubeTask
	var workerMachineName string

	// 尝试解析请求体
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	// 先尝试解析为新的格式（包含 machine_name 的对象）
	if err := json.Unmarshal(bodyBytes, &req); err == nil && req.Updates != nil && len(req.Updates) > 0 {
		// 新格式：包含 updates 和 machine_name
		updates = req.Updates
		workerMachineName = req.MachineName
	} else {
		// 旧格式：直接是数组
		if err := json.Unmarshal(bodyBytes, &updates); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format: " + err.Error()})
			return
		}
		workerMachineName = "" // 旧格式没有 machine_name
	}

	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No updates"})
		return
	}

	log.Printf("[BatchUpdate] Received %d task updates", len(updates))
	for i, u := range updates {
		log.Printf("[BatchUpdate] Update %d: task_id=%d, status=%s, video_id='%s', audio_url=%v, video_url=%v, title='%s'",
			i+1, u.ID, u.Status, u.VideoID, u.AudioURL != "", u.VideoURL != "", u.Title)
	}

	ctx := context.Background()

	// 1. Fetch existing tasks to merge updates
	var keys []string
	for _, u := range updates {
		keys = append(keys, fmt.Sprintf("task:%d", u.ID))
	}

	// 使用带超时的 context 进行 MGet 操作，避免长时间阻塞
	mgetCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	existingJSONs, err := database.RDB.MGet(mgetCtx, keys...).Result()
	if err != nil {
		log.Printf("[BatchUpdate] MGet failed: %v (keys count: %d)", err, len(keys))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()

	for i, u := range updates {
		val := existingJSONs[i]
		if val == nil {
			continue // Task not found
		}

		var existing models.YoutubeTask
		if err := json.Unmarshal([]byte(val.(string)), &existing); err != nil {
			// Try flexible time parser for tasks with non-standard time formats
			log.Printf("[BatchUpdate] Standard unmarshal failed for task %d: %v, trying flexible parser", u.ID, err)
			if fixedTask, fixErr := unmarshalYoutubeTaskWithFlexibleTime([]byte(val.(string))); fixErr == nil {
				log.Printf("[BatchUpdate] Flexible parser succeeded for task %d (status: %s)", u.ID, fixedTask.Status)
				existing = fixedTask
			} else {
				log.Printf("[BatchUpdate] Flexible parser also failed for task %d: %v, skipping update", u.ID, fixErr)
				continue
			}
		}

		oldStatus := existing.Status
		// 记录更新前的字段值，用于调试
		oldURL := existing.URL
		oldTitle := existing.Title
		oldVideoID := existing.VideoID

		log.Printf("[BatchUpdate] Processing task %d: old_status=%s, update_status=%s, update_video_id='%s', update_audio_url=%v, update_video_url=%v, update_title='%s'",
			u.ID, oldStatus, u.Status, u.VideoID, u.AudioURL != "", u.VideoURL != "", u.Title)
		log.Printf("[BatchUpdate] Task %d before update: url='%s', title='%s', video_id='%s'",
			u.ID, oldURL, oldTitle, oldVideoID)

		// 2. Merge fields (only update if provided/valid)
		if u.Status != "" {
			existing.Status = u.Status
		}
		if u.ErrorMessage != "" {
			existing.ErrorMessage = u.ErrorMessage
		}
		if u.WorkerID != "" {
			existing.WorkerID = u.WorkerID
		}
		if u.IsDownloadFail {
			existing.IsDownloadFail = true
		}
		// Clear error/failure flags if restarting
		// METADATA_PROCESSING 是 metadata worker 使用的状态，也需要清除错误标志
		if u.Status == "RUNNING" || u.Status == "PENDING" || u.Status == "METADATA_PROCESSING" {
			existing.IsDownloadFail = false
			existing.ErrorMessage = ""
		}
		// URL 字段：只在有提供时才更新，否则保留原有值（确保 URL 不会丢失）
		// 重要：不要清空 URL，即使更新请求中没有 URL 字段
		if u.URL != "" {
			existing.URL = u.URL
		}
		// 如果原有 URL 存在但更新请求中没有 URL，确保保留
		if existing.URL == "" && oldURL != "" {
			log.Printf("[BatchUpdate] WARNING: Task %d URL would be lost (old='%s'), preserving it", u.ID, oldURL)
			existing.URL = oldURL
		}
		// Metadata fields - 只在有提供时才更新，否则保留原有值
		if u.AudioURL != "" {
			existing.AudioURL = u.AudioURL
		}
		if u.AudioSize != 0 {
			existing.AudioSize = u.AudioSize
		}
		if u.VideoURL != "" {
			existing.VideoURL = u.VideoURL
		}
		if u.VideoSize != 0 {
			existing.VideoSize = u.VideoSize
		}
		// Title 和 VideoID：只在有提供时才更新，否则保留原有值
		if u.Title != "" {
			existing.Title = u.Title
		}
		// 如果原有 Title 存在但更新请求中没有 Title，确保保留
		if existing.Title == "" && oldTitle != "" {
			log.Printf("[BatchUpdate] WARNING: Task %d Title would be lost (old='%s'), preserving it", u.ID, oldTitle)
			existing.Title = oldTitle
		}
		if u.VideoID != "" {
			existing.VideoID = u.VideoID
		}
		// 如果原有 VideoID 存在但更新请求中没有 VideoID，确保保留
		if existing.VideoID == "" && oldVideoID != "" {
			log.Printf("[BatchUpdate] WARNING: Task %d VideoID would be lost (old='%s'), preserving it", u.ID, oldVideoID)
			existing.VideoID = oldVideoID
		}

		log.Printf("[BatchUpdate] Task %d after update: url='%s', title='%s', video_id='%s'",
			u.ID, existing.URL, existing.Title, existing.VideoID)

		// Track Status Change
		if u.Status != "" && existing.JobID != 0 {
			trackStatusChange(existing.JobID, oldStatus, existing.Status)
		}

		existing.UpdatedAt = time.Now()
		// METADATA_PROCESSING 和 RUNNING 都表示任务正在处理中，需要设置 StartedAt
		if (u.Status == "RUNNING" || u.Status == "METADATA_PROCESSING") && existing.StartedAt.IsZero() {
			existing.StartedAt = time.Now()
		}
		if (u.Status == "COMPLETED" || u.Status == "FAILED") && existing.CompletedAt.IsZero() {
			existing.CompletedAt = time.Now()
		}

		// 3. Save back
		data, err := json.Marshal(existing)
		if err != nil {
			log.Printf("[BatchUpdate] Failed to marshal task %d: %v", existing.ID, err)
			continue
		}

		taskKey := fmt.Sprintf("task:%d", existing.ID)
		pipe.Set(ctx, taskKey, data, 0)
		log.Printf("[BatchUpdate] ✓ Saved task %d: status=%s, video_id='%s', audio_url=%v, video_url=%v",
			existing.ID, existing.Status, existing.VideoID, existing.AudioURL != "", existing.VideoURL != "")

		// Ensure in Job ZSet (idempotent)
		if existing.JobID != 0 {
			jobKey := fmt.Sprintf("job:%d:tasks", existing.JobID)
			pipe.ZAdd(ctx, jobKey, redis.Z{
				Score:  float64(existing.ID),
				Member: existing.ID,
			})
		}

		// Status Machine Transition: METADATA_FETCHED -> Ready for Download
		// Only push to queue if status is being set to METADATA_FETCHED (not if it's already METADATA_FETCHED)
		// Also check if task has URLs (audio_url or video_url) before pushing to download queue
		if u.Status == "METADATA_FETCHED" && oldStatus != "METADATA_FETCHED" {
			// Only push if task has URLs (metadata was successfully fetched)
			if existing.AudioURL != "" || existing.VideoURL != "" {
				// 确定推入哪个下载队列
				var queueName string
				jobMachineName := getJobMachineName(existing.JobID)
				if jobMachineName == "" {
					// Job 的 MachineName 为空，如果提供了 worker 的 machine_name，推入到该机器的队列
					// 这样可以确保 metadata 和 download 在同一台机器上完成
					if workerMachineName != "" {
						queueName = fmt.Sprintf("queue:youtube:download_ready:%s", workerMachineName)
						log.Printf("[BatchUpdate] Job %d has no machine_name, pushing task %d to worker's queue: %s", existing.JobID, existing.ID, queueName)
					} else {
						// 如果没有提供 worker 的 machine_name，推入 all 队列
						queueName = "queue:youtube:download_ready:all"
					}
				} else {
					// Job 有指定的 MachineName，推入到对应的队列
					queueName = fmt.Sprintf("queue:youtube:download_ready:%s", jobMachineName)
				}
				pipe.RPush(ctx, queueName, existing.ID)
				log.Printf("[BatchUpdate] Pushed task %d to download queue: %s (job_id: %d, job_machine: %s, worker_machine: %s)",
					existing.ID, queueName, existing.JobID, jobMachineName, workerMachineName)
			} else {
				log.Printf("[BatchUpdate] Task %d status changed to METADATA_FETCHED but has no URLs (audio_url=%v, video_url=%v), skipping queue push",
					existing.ID, existing.AudioURL != "", existing.VideoURL != "")
			}
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update tasks: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// Job machine name cache to avoid repeated DB queries
var jobMachineNameCache = sync.Map{} // map[int64]string

// getJobMachineName 获取 Job 的主机名（带缓存）
func getJobMachineName(jobID int64) string {
	// 先查缓存
	if cached, ok := jobMachineNameCache.Load(jobID); ok {
		return cached.(string)
	}

	// 查询数据库
	var job models.YoutubeJob
	if err := database.DB.Select("machine_name").First(&job, jobID).Error; err != nil {
		// 如果查询失败，返回空字符串（允许所有主机处理）
		jobMachineNameCache.Store(jobID, "")
		return ""
	}

	// 缓存结果
	jobMachineNameCache.Store(jobID, job.MachineName)
	return job.MachineName
}

// shouldProcessTask 检查任务是否应该被当前主机处理
// 注意：这个函数主要用于 job buffers 中的任务过滤（因为 buffers 是预填充的，可能包含其他机器的任务）
func shouldProcessTask(taskJobID int64, workerMachineName string) bool {
	jobMachineName := getJobMachineName(taskJobID)
	// 如果 Job 的 MachineName 为空，所有主机都可以处理
	if jobMachineName == "" {
		return true
	}
	// 如果 Job 的 MachineName 与 worker 的主机名匹配，可以处理
	return jobMachineName == workerMachineName
}

// getDownloadQueueName 根据 Job 的 MachineName 获取下载队列名
// 如果 MachineName 为空，返回 "all" 队列（所有机器都可以处理）
// 否则返回特定机器的队列
func getDownloadQueueName(jobID int64) string {
	machineName := getJobMachineName(jobID)
	if machineName == "" {
		return "queue:youtube:download_ready:all"
	}
	// 使用安全的队列名（避免特殊字符）
	return fmt.Sprintf("queue:youtube:download_ready:%s", machineName)
}

// getMetadataQueueName 根据 Job 的 MachineName 获取 metadata 队列名
// 如果 MachineName 为空，返回 "all" 队列（所有机器都可以处理）
// 否则返回特定机器的队列
func getMetadataQueueName(jobID int64) string {
	machineName := getJobMachineName(jobID)
	if machineName == "" {
		return "queue:youtube:metadata_retry:all"
	}
	// 使用安全的队列名（避免特殊字符）
	return fmt.Sprintf("queue:youtube:metadata_retry:%s", machineName)
}

func AcquireTasks(c *gin.Context) {
	type AcquireRequest struct {
		WorkerID    string `json:"worker_id"`
		MachineName string `json:"machine_name"` // 可选：worker 的主机名
		Stage       string `json:"stage"`        // "metadata" (default) or "download"
		Limit       int    `json:"limit"`
	}

	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.Stage == "" {
		req.Stage = "metadata"
	}

	log.Printf("[AcquireTasks] Worker %s (machine: %s) requesting %d tasks (stage: %s)", req.WorkerID, req.MachineName, req.Limit, req.Stage)

	ctx := context.Background()
	tasks := []models.YoutubeTask{}
	seenTaskIDs := map[string]struct{}{}

	if req.Stage == "download" {
		// 根据 worker 的 MachineName 确定从哪个队列获取任务
		// 如果 MachineName 为空，只从 "all" 队列获取
		// 如果 MachineName 不为空，先从特定机器队列获取，如果为空再从 "all" 队列获取
		var queueNames []string
		if req.MachineName != "" {
			// 优先从特定机器的队列获取
			queueNames = []string{
				fmt.Sprintf("queue:youtube:download_ready:%s", req.MachineName),
				"queue:youtube:download_ready:all", // 如果特定队列为空，再从 all 队列获取
			}
		} else {
			// 如果没有指定 MachineName，只从 all 队列获取
			queueNames = []string{"queue:youtube:download_ready:all"}
		}

		// 检查队列长度（用于调试）
		for _, queueName := range queueNames {
			length, _ := database.RDB.LLen(ctx, queueName).Result()
			if length > 0 {
				log.Printf("[AcquireTasks] Queue %s has %d tasks (worker: %s, machine: %s)", queueName, length, req.WorkerID, req.MachineName)
			}
		}

		// 从队列中获取任务
		for i := 0; i < req.Limit; i++ {
			var idStr string
			var err error

			// 尝试从各个队列获取任务
			for _, queueName := range queueNames {
				idStr, err = database.RDB.LPop(ctx, queueName).Result()
				if err == nil {
					// 成功获取到任务
					break
				} else if err != redis.Nil {
					// 其他错误，记录并继续
					log.Printf("[AcquireTasks] Error popping from queue %s: %v", queueName, err)
					continue
				}
				// redis.Nil 表示队列为空，继续尝试下一个队列
			}

			if err == redis.Nil {
				// 所有队列都为空
				break
			}
			if err != nil {
				// 其他错误，跳过
				continue
			}
			if _, exists := seenTaskIDs[idStr]; exists {
				log.Printf("[AcquireTasks] Skip duplicate task id %s in this batch (worker: %s, stage: %s)", idStr, req.WorkerID, req.Stage)
				continue
			}
			seenTaskIDs[idStr] = struct{}{}

			// Fetch task details
			taskData, err := database.RDB.Get(ctx, fmt.Sprintf("task:%s", idStr)).Result()
			if err == nil {
				var t models.YoutubeTask
				if err := json.Unmarshal([]byte(taskData), &t); err == nil {
					// 不再需要检查主机名过滤，因为已经通过队列分离了
					// Optimization: Check if actually METADATA_FETCHED?
					// Ideally yes, but queue implies readiness.
					tasks = append(tasks, t)
				}
			}
		}
	} else {
		// Metadata stage

		// 1. Check Retry Queue (根据机器名分离队列)
		// 根据 worker 的 MachineName 确定从哪个队列获取任务
		var queueNames []string
		if req.MachineName != "" {
			// 优先从特定机器的队列获取
			queueNames = []string{
				fmt.Sprintf("queue:youtube:metadata_retry:%s", req.MachineName),
				"queue:youtube:metadata_retry:all", // 如果特定队列为空，再从 all 队列获取
			}
		} else {
			// 如果没有指定 MachineName，只从 all 队列获取
			queueNames = []string{"queue:youtube:metadata_retry:all"}
		}

		// 检查队列长度（用于日志）
		totalQueueLength := int64(0)
		for _, queueName := range queueNames {
			length, _ := database.RDB.LLen(ctx, queueName).Result()
			totalQueueLength += length
		}
		if totalQueueLength > 0 {
			log.Printf("[AcquireTasks] Retry queues have %d tasks total, worker %s requesting %d tasks (stage: %s)", totalQueueLength, req.WorkerID, req.Limit, req.Stage)
		} else if req.Stage == "metadata" {
			log.Printf("[AcquireTasks] Retry queues are empty, worker %s will check job buffers", req.WorkerID)
		}

		for len(tasks) < req.Limit {
			// 使用非阻塞 LPOP 轮询队列，避免“machine 专属队列为空、all 队列有数据”时每次都被 1s 阻塞
			// 这样可以显著降低 /tasks/acquire 的尾延迟，避免 worker 10s read timeout
			// LPOP 同样是原子操作，不会出现多 worker 重复弹出同一个任务
			var idStr string
			var queueName string
			var found bool

			// 顺序尝试队列：先 machine 队列，再 all 队列
			for _, qName := range queueNames {
				id, err := database.RDB.LPop(ctx, qName).Result()
				if err == redis.Nil {
					// 队列为空，尝试下一个队列
					continue
				}
				if err != nil {
					log.Printf("[AcquireTasks] LPOP error from queue %s: %v (worker: %s)", qName, err, req.WorkerID)
					continue
				}
				queueName = qName
				idStr = id
				found = true
				if len(tasks) == 0 {
					log.Printf("[AcquireTasks] Popped first task %s from retry queue %s (worker: %s)", idStr, queueName, req.WorkerID)
				}
				break
			}

			if !found {
				// All queues are empty or timed out
				log.Printf("[AcquireTasks] All retry queues empty or timed out (worker: %s)", req.WorkerID)
				break
			}
			if _, exists := seenTaskIDs[idStr]; exists {
				log.Printf("[AcquireTasks] Skip duplicate task id %s in this batch (worker: %s, stage: %s)", idStr, req.WorkerID, req.Stage)
				continue
			}
			seenTaskIDs[idStr] = struct{}{}

			// Check task status after atomic pop
			taskData, err := database.RDB.Get(ctx, fmt.Sprintf("task:%s", idStr)).Result()
			if err != nil {
				// Task not found, already removed from queue by BLPOP, skip
				log.Printf("[AcquireTasks] Task %s not found in Redis, skipping", idStr)
				continue
			}

			var t models.YoutubeTask
			if err := json.Unmarshal([]byte(taskData), &t); err != nil {
				// Try flexible time parser for tasks with non-standard time formats
				log.Printf("[AcquireTasks] Standard unmarshal failed for task %s: %v, trying flexible parser", idStr, err)
				if fixedTask, fixErr := unmarshalYoutubeTaskWithFlexibleTime([]byte(taskData)); fixErr == nil {
					log.Printf("[AcquireTasks] Flexible parser succeeded for task %s (status: %s)", idStr, fixedTask.Status)
					t = fixedTask
				} else {
					// Both parsers failed, skip this task
					log.Printf("[AcquireTasks] Flexible parser also failed for task %s: %v, skipping", idStr, fixErr)
					continue
				}
			}

			if t.Status == "COMPLETED" {
				log.Printf("[AcquireTasks] Task %s status is COMPLETED, skipping", idStr)
				continue
			}
			if t.Status != "PENDING" {
				t.Status = "PENDING"
				t.WorkerID = ""
				t.ErrorMessage = ""
				t.UpdatedAt = time.Now()
				if data, merr := json.Marshal(t); merr == nil {
					database.RDB.Set(ctx, fmt.Sprintf("task:%s", idStr), data, 0)
				}
			}
			log.Printf("[AcquireTasks] Task %s (job %d) accepted from retry queue for worker %s", idStr, t.JobID, req.WorkerID)
			tasks = append(tasks, t)
		}

		if len(tasks) > 0 {
			log.Printf("[AcquireTasks] Returning %d tasks from retry queue to worker %s", len(tasks), req.WorkerID)
		}

		if len(tasks) >= req.Limit {
			log.Printf("[AcquireTasks] Returning %d tasks to worker %s (limit reached)", len(tasks), req.WorkerID)
			c.JSON(http.StatusOK, tasks)
			return
		}

		// 2. Round-robin across active job buffers
		// We need to iterate over map.
		bufferMutex.RLock()
		// Get keys
		var jobIDs []int64
		for jid := range jobBuffers {
			jobIDs = append(jobIDs, jid)
		}
		bufferMutex.RUnlock()

		if len(jobIDs) == 0 {
			c.JSON(http.StatusOK, tasks)
			return
		}

		// Simple strategy: Try each job until we have enough tasks
		for _, jid := range jobIDs {
			if len(tasks) >= req.Limit {
				break
			}

			bufferMutex.RLock()
			ch, ok := jobBuffers[jid]
			bufferMutex.RUnlock()

			if !ok {
				continue
			}

			// Proactive refill
			if len(ch) < BufferLowWater {
				triggerRefill(jid)
			}

			// Drain what we can
			// 注意：job buffers 中的任务在填充时没有按机器名过滤
			// 但这里我们可以检查，如果不匹配就跳过（不会放回 buffer，因为 buffer 是预填充的）
		loop:
			for len(tasks) < req.Limit {
				select {
				case t := <-ch:
					// Check if status is PENDING?
					// The buffer *should* ideally only contain pending, but if we have restarts...
					// For now assume buffer is raw tasks from ZRange.
					// We might want to check status if we want strict PENDING.
					// But let's assume Python worker handles idempotency.

					// 检查主机名过滤（job buffers 中的任务需要检查）
					if req.MachineName != "" {
						if !shouldProcessTask(t.JobID, req.MachineName) {
							// 不匹配，跳过这个任务（不放回 buffer，因为 buffer 是预填充的，可能包含其他机器的任务）
							continue
						}
					}
					tasks = append(tasks, t)
				default:
					break loop
				}
			}
		}
	}

	log.Printf("[AcquireTasks] Final: Returning %d tasks to worker %s (stage: %s)", len(tasks), req.WorkerID, req.Stage)
	c.JSON(http.StatusOK, tasks) // Direct array response
}

func BatchFetch(c *gin.Context) {
	type FetchRequest struct {
		JobID  int64  `json:"job_id"`
		Limit  int    `json:"limit"`
		Offset int    `json:"offset"`
		Status string `json:"status"`
	}

	var req FetchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()

	// 从 MySQL 查询总数
	var total int64
	countQuery := database.DB.Model(&models.YoutubeTaskRecord{}).
		Where("job_id = ?", req.JobID)
	if req.Status != "" {
		countQuery = countQuery.Where("status = ?", req.Status)
	}
	if err := countQuery.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get count: " + err.Error()})
		return
	}

	if total == 0 {
		c.JSON(http.StatusOK, gin.H{"tasks": []models.YoutubeTask{}, "total": 0})
		return
	}

	// 从 MySQL 查询任务记录（分页）
	var taskRecords []models.YoutubeTaskRecord
	queryCtx, queryCancel := context.WithTimeout(ctx, 30*time.Second)
	defer queryCancel()

	taskQuery := database.DB.WithContext(queryCtx).
		Where("job_id = ?", req.JobID)
	if req.Status != "" {
		taskQuery = taskQuery.Where("status = ?", req.Status)
	}
	if err := taskQuery.
		Order("id ASC").
		Offset(req.Offset).
		Limit(req.Limit).
		Find(&taskRecords).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch tasks: " + err.Error()})
		return
	}

	// 将 YoutubeTaskRecord 转换为 YoutubeTask
	var tasks []models.YoutubeTask
	for _, record := range taskRecords {
		task := models.YoutubeTask{
			ID:           int64(record.ID),
			JobID:        int64(record.JobID),
			URL:          record.URL,
			Status:       record.Status,
			WorkerID:     record.WorkerID,
			Title:        record.Title,
			VideoID:      record.VideoID,
			AudioURL:     record.AudioURL,
			AudioSize:    record.AudioSize,
			VideoURL:     record.VideoURL,
			VideoSize:    record.VideoSize,
			ErrorMessage: record.ErrorMessage,
			CreatedAt:    record.CreatedAt,
			UpdatedAt:    record.UpdatedAt,
		}
		tasks = append(tasks, task)
	}

	// Log summary
	if len(tasks) > 0 {
		log.Printf("INFO: BatchFetch for job %d: successfully fetched %d/%d tasks from MySQL (offset: %d, limit: %d, total: %d)",
			req.JobID, len(tasks), len(taskRecords), req.Offset, req.Limit, total)
	}

	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "total": total})
}

func BatchDelete(c *gin.Context) {
	type DeleteRequest struct {
		IDs []int64 `json:"ids"`
	}
	var req DeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.IDs) == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "no ids"})
		return
	}

	ctx := context.Background()

	// First, we need to know which job these tasks belong to, to remove from ZSet.
	// We can MGet them.
	var keys []string
	for _, id := range req.IDs {
		keys = append(keys, fmt.Sprintf("task:%d", id))
	}

	// We handle this in chunks or just MGet all. Assuming reasonable batch size.
	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch tasks for deletion: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()

	for i, item := range jsonList {
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			// Remove from Job ZSet
			jobKey := fmt.Sprintf("job:%d:tasks", task.JobID)
			pipe.ZRem(ctx, jobKey, task.ID)
		}

		// Remove Task Key
		pipe.Del(ctx, keys[i])
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete tasks: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

// AddTasksToJob adds new tasks to an existing job in Redis
func AddTasksToJob(jobID int64, urls []string) (int, error) {
	log.Printf("[AddTasksToJob] Function called for job %d with %d URLs", jobID, len(urls))

	if len(urls) == 0 {
		log.Printf("[AddTasksToJob] No URLs provided for job %d, returning", jobID)
		return 0, nil
	}

	ctx := context.Background()
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)
	// 将新任务也推入 metadata 队列（便于 worker 直接从队列获取，不依赖 buffer 扫描）
	// 复用 metadata_retry 队列（按 machine_name 分流），对“新任务”和“重试任务”都适用：只要 status=PENDING 就会被处理
	metadataQueueName := getMetadataQueueName(jobID)

	log.Printf("[AddTasksToJob] Starting ID determination for job %d", jobID)
	// 1. Determine start ID
	// Try to get the last ID from ZSet to continue sequence (支持向已有 Job 添加任务)
	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()
	var startID int64
	if err != nil || len(lastIDs) == 0 {
		// No tasks yet, use default start: jobID * 1000000
		startID = jobID * 1000000
		log.Printf("[AddTasksToJob] No existing tasks for job %d, using default startID: %d", jobID, startID)
	} else {
		// Parse last ID and continue from next
		// Redis returns string member
		fmt.Sscanf(lastIDs[0], "%d", &startID)
		startID++ // Start from next
		log.Printf("[AddTasksToJob] Found existing tasks for job %d, continuing from startID: %d", jobID, startID)
	}

	now := time.Now()
	log.Printf("[AddTasksToJob] Starting data insertion for job %d (startID: %d, URLs: %d)", jobID, startID, len(urls))
	// 减小批次大小，避免 Redis pipeline 超时（10000 太大，容易超时）
	batchSize := 2000
	totalAdded := 0

	log.Printf("[AddTasksToJob] Starting to process %d URLs for job %d (batch size: %d)", len(urls), jobID, batchSize)

	// 1. 先全部写入 MySQL（分批处理，但先完成所有 MySQL 写入）
	totalBatches := (len(urls) + batchSize - 1) / batchSize
	log.Printf("[AddTasksToJob] Will process %d batches for job %d (MySQL first, then Redis)", totalBatches, jobID)

	// 收集所有需要写入 MySQL 的记录
	allTaskRecords := make([]models.YoutubeTaskRecord, 0, len(urls))

	// 第一阶段：全部写入 MySQL
	for i := 0; i < len(urls); i += batchSize {
		end := i + batchSize
		if end > len(urls) {
			end = len(urls)
		}

		batchUrls := urls[i:end]
		batchNum := i/batchSize + 1

		log.Printf("[AddTasksToJob] [MySQL Phase] Processing batch %d/%d for job %d (%d URLs, range: %d-%d)",
			batchNum, totalBatches, jobID, len(batchUrls), i, end-1)

		var batchTaskRecords []models.YoutubeTaskRecord // 收集这批的数据库记录

		for j, url := range batchUrls {
			taskID := startID + int64(i+j)

			// 收集数据库记录，包含原始 URL
			// VideoID 会在 metadata worker 处理时提取
			taskRecord := models.YoutubeTaskRecord{
				ID:        uint(taskID),
				JobID:     uint(jobID),
				URL:       url, // 记录原始的 YouTube URL
				Status:    "PENDING",
				CreatedAt: now,
				UpdatedAt: now,
			}
			batchTaskRecords = append(batchTaskRecords, taskRecord)
		}

		// 批量写入 MySQL
		if len(batchTaskRecords) > 0 {
			batchSizeDB := 1000
			successCount := 0
			for dbIdx := 0; dbIdx < len(batchTaskRecords); dbIdx += batchSizeDB {
				dbEnd := dbIdx + batchSizeDB
				if dbEnd > len(batchTaskRecords) {
					dbEnd = len(batchTaskRecords)
				}
				dbBatch := batchTaskRecords[dbIdx:dbEnd]

				log.Printf("[AddTasksToJob] Attempting to insert %d task records to MySQL for job %d batch %d/%d (task IDs: %d-%d)",
					len(dbBatch), jobID, batchNum, totalBatches, dbBatch[0].ID, dbBatch[len(dbBatch)-1].ID)

				// 使用带超时的 context 进行 MySQL 写入，避免长时间阻塞
				dbCtx, dbCancel := context.WithTimeout(ctx, 30*time.Second)
				startTime := time.Now()

				err := database.DB.WithContext(dbCtx).Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(&dbBatch).Error

				duration := time.Since(startTime)
				dbCancel()

				if err != nil {
					log.Printf("[AddTasksToJob] ERROR: Failed to insert task records for job %d (batch %d/%d): %v (took %v)",
						jobID, batchNum, totalBatches, err, duration)
					// 继续处理下一批，不中断整个流程
				} else {
					successCount += len(dbBatch)
					if duration > 2*time.Second {
						log.Printf("[AddTasksToJob] WARNING: MySQL insert for job %d batch %d/%d took %v (may be blocking other operations)",
							jobID, batchNum, totalBatches, duration)
					}
					log.Printf("[AddTasksToJob] Successfully inserted %d task records to MySQL for job %d batch %d/%d (took %v)",
						len(dbBatch), jobID, batchNum, totalBatches, duration)
				}
			}

			// 收集这批的数据库记录（用于后续统计和 Redis 写入）
			// 即使 MySQL 写入失败，我们仍然会写入 Redis（因为 Redis 是主要的工作队列）
			allTaskRecords = append(allTaskRecords, batchTaskRecords...)
			totalAdded += len(batchTaskRecords)
			log.Printf("[AddTasksToJob] [MySQL Phase] Completed batch %d/%d for job %d. MySQL inserted: %d/%d, Total added so far: %d",
				batchNum, totalBatches, jobID, successCount, len(batchTaskRecords), totalAdded)
		}
	}

	log.Printf("[AddTasksToJob] [MySQL Phase] Completed database insertion for job %d (%d records total)", jobID, len(allTaskRecords))

	// 第二阶段：批量写入 Redis（所有 MySQL 写入成功后才开始）
	if len(allTaskRecords) > 0 {
		log.Printf("[AddTasksToJob] [Redis Phase] Starting Redis insertion for job %d (%d records total)", jobID, len(allTaskRecords))

		// 重新遍历 URLs，写入 Redis
		for i := 0; i < len(urls); i += batchSize {
			end := i + batchSize
			if end > len(urls) {
				end = len(urls)
			}

			batchUrls := urls[i:end]
			batchNum := i/batchSize + 1

			log.Printf("[AddTasksToJob] [Redis Phase] Processing batch %d/%d for job %d (%d URLs, range: %d-%d)",
				batchNum, totalBatches, jobID, len(batchUrls), i, end-1)

			var zMembers []redis.Z
			var taskKeys []string
			pipe := database.RDB.Pipeline()
			queueValues := make([]interface{}, 0, len(batchUrls))

			for j, url := range batchUrls {
				taskID := startID + int64(i+j)
				task := models.YoutubeTask{
					ID:        taskID,
					JobID:     jobID,
					URL:       url,
					Status:    "PENDING",
					CreatedAt: now,
					UpdatedAt: now,
				}

				data, err := json.Marshal(task)
				if err != nil {
					log.Printf("WARNING: Failed to marshal task %d for job %d: %v", taskID, jobID, err)
					continue
				}

				taskKey := fmt.Sprintf("task:%d", task.ID)
				pipe.Set(ctx, taskKey, data, 0)
				taskKeys = append(taskKeys, taskKey)

				zMembers = append(zMembers, redis.Z{
					Score:  float64(task.ID),
					Member: task.ID,
				})

				// 推入 metadata 队列（使用 string，保证 BLPOP/LPOP 得到的就是字符串 task id）
				queueValues = append(queueValues, fmt.Sprintf("%d", task.ID))
			}

			if len(zMembers) > 0 {
				// Optimize: Single ZAdd for the whole batch
				pipe.ZAdd(ctx, jobKey, zMembers...)
				// 让 metadata worker 能直接从队列获取新任务
				if len(queueValues) > 0 {
					pipe.RPush(ctx, metadataQueueName, queueValues...)
				}

				// 添加重试机制，处理临时网络问题（如超时）
				var err error
				maxRetries := 3
				for retry := 0; retry < maxRetries; retry++ {
					_, err = pipe.Exec(ctx)
					if err == nil {
						break
					}

					// 检查是否是超时错误或其他可重试的错误
					if retry < maxRetries-1 {
						waitTime := time.Duration(retry+1) * 2 * time.Second // 递增等待时间：2s, 4s, 6s
						log.Printf("[AddTasksToJob] WARNING: Redis pipeline failed for job %d batch %d/%d (retry %d/%d): %v. Retrying in %v...",
							jobID, batchNum, totalBatches, retry+1, maxRetries, err, waitTime)
						time.Sleep(waitTime)

						// 重新创建 pipeline（因为之前的 pipeline 已经执行过了）
						pipe = database.RDB.Pipeline()
						// 重新设置所有 task keys（可能已经部分写入，需要确保完整性）
						queueValues = queueValues[:0]
						for j, url := range batchUrls {
							taskID := startID + int64(i+j)
							task := models.YoutubeTask{
								ID:        taskID,
								JobID:     jobID,
								URL:       url,
								Status:    "PENDING",
								CreatedAt: now,
								UpdatedAt: now,
							}
							data, _ := json.Marshal(task)
							pipe.Set(ctx, fmt.Sprintf("task:%d", taskID), data, 0)
							queueValues = append(queueValues, fmt.Sprintf("%d", taskID))
						}
						pipe.ZAdd(ctx, jobKey, zMembers...)
						if len(queueValues) > 0 {
							pipe.RPush(ctx, metadataQueueName, queueValues...)
						}
					}
				}

				if err != nil {
					log.Printf("[AddTasksToJob] ERROR: Failed to execute Redis pipeline for job %d batch %d/%d after %d retries: %v. %d task keys may have been created but not added to sorted set",
						jobID, batchNum, totalBatches, maxRetries, err, len(taskKeys))
					log.Printf("[AddTasksToJob] ERROR: Stopping Redis phase for job %d. Processed %d/%d batches, added %d tasks so far",
						jobID, batchNum-1, totalBatches, totalAdded)
					// Try to clean up any task keys that were created but not added to sorted set
					// This is best-effort cleanup
					cleanupPipe := database.RDB.Pipeline()
					for _, key := range taskKeys {
						cleanupPipe.Del(ctx, key)
					}
					cleanupPipe.Exec(ctx)
					return totalAdded, fmt.Errorf("failed at Redis batch %d/%d: %w", batchNum, totalBatches, err)
				}

				log.Printf("[AddTasksToJob] Successfully added %d tasks to Redis for job %d batch %d/%d", len(zMembers), jobID, batchNum, totalBatches)
				log.Printf("[AddTasksToJob] [Redis Phase] Completed batch %d/%d for job %d. Total added so far: %d", batchNum, totalBatches, jobID, totalAdded)
			}
		}

		log.Printf("[AddTasksToJob] [Redis Phase] Completed Redis insertion for job %d (%d records total)", jobID, len(allTaskRecords))
		log.Printf("[AddTasksToJob] [Redis Phase] Queued new tasks to metadata queue: %s (job %d)", metadataQueueName, jobID)
	} else {
		log.Printf("[AddTasksToJob] WARNING: No task records to insert for job %d", jobID)
	}

	log.Printf("[AddTasksToJob] Completed processing all batches for job %d. Total tasks added: %d", jobID, totalAdded)
	return totalAdded, nil
}

type TransferTaskInput struct {
	Src  string `json:"src"`
	Size int64  `json:"size"`
}

func AddTransferTasksToJob(jobID int64, inputs []TransferTaskInput) (int, error) {
	if len(inputs) == 0 {
		return 0, nil
	}

	// 内存去重 (无论新旧都需要)
	uniqueInputs := make([]TransferTaskInput, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if !seen[input.Src] {
			seen[input.Src] = true
			uniqueInputs = append(uniqueInputs, input)
		}
	}
	inputs = uniqueInputs

	if isJobSharded(context.Background(), jobID) {
		return addShardedTransferTasks(jobID, inputs) // 【新】分片逻辑
	} else {
		return addLegacyTransferTasks(jobID, inputs) // 【旧】保持原有逻辑
	}
}

// 【新逻辑】分片写入
func addShardedTransferTasks(jobID int64, inputs []TransferTaskInput) (int, error) {
	ctx := context.Background()
	pipe := database.RDB.Pipeline()

	// 1. Redis 去重 (分片检查)
	// 使用 Pipeline 批量检查不同分片的 HExists
	dedupCmds := make([]*redis.BoolCmd, len(inputs))
	for i, input := range inputs {
		key := getDedupShardKey(jobID, input.Src)
		dedupCmds[i] = pipe.HExists(ctx, key, input.Src)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	var newInputs []TransferTaskInput
	for i, cmd := range dedupCmds {
		if !cmd.Val() {
			newInputs = append(newInputs, inputs[i])
		}
	}

	if len(newInputs) == 0 {
		return 0, nil
	}

	// 2. 生成 ID (使用独立的计数器 Key)
	// tx:job:11:max_id
	idCounterKey := fmt.Sprintf("tx:job:%d:max_id", jobID)
	endID, err := database.RDB.IncrBy(ctx, idCounterKey, int64(len(newInputs))).Result()
	if err != nil {
		return 0, err
	}
	startID := endID - int64(len(newInputs)) + 1

	// 3. 批量写入 (分片 ZSet + 分片 Dedup)
	pipe = database.RDB.Pipeline()
	now := time.Now()
	var tasks []models.TransferTask

	for i, input := range newInputs {
		taskID := startID + int64(i)

		task := models.TransferTask{
			ID:        taskID,
			JobID:     jobID,
			Src:       input.Src,
			Size:      input.Size,
			Status:    "PENDING",
			CreatedAt: now,
			UpdatedAt: now,
		}
		tasks = append(tasks, task)

		data, _ := json.Marshal(task)

		// 3.1 任务详情 (String) - 可以保持原样，或也分片，这里沿用原命名习惯
		taskKey := fmt.Sprintf("tx:task:%d:%d", jobID, task.ID)
		pipe.Set(ctx, taskKey, data, 0)

		// 3.2 任务队列 (分桶 ZSet)
		bucketKey := getTaskBucketKey(jobID, task.ID)
		pipe.ZAdd(ctx, bucketKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})

		// 3.3 去重记录 (分片 Hash)
		dedupShard := getDedupShardKey(jobID, input.Src)
		pipe.HSet(ctx, dedupShard, input.Src, task.ID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	// 4. 批量写入 PostgreSQL (transfer_tasks 表)
	if len(tasks) > 0 {
		for _, task := range tasks {
			result := database.DB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},
				DoNothing: true,
			}).Create(&task)
			if result.Error != nil {
				log.Printf("[AddTasks] Failed to insert task %d for job %d: %v", task.ID, jobID, result.Error)
			}
		}
	}

	var totalSizeBytes int64
	for _, input := range newInputs {
		if input.Size > 0 {
			totalSizeBytes += input.Size
		}
	}
	if totalSizeBytes > 0 {
		database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).
			UpdateColumn("total_size_bytes", gorm.Expr("total_size_bytes + ?", totalSizeBytes))
	}

	if len(newInputs) > 0 {
		database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).
			Updates(map[string]interface{}{
				"total_count":   gorm.Expr("total_count + ?", len(newInputs)),
				"pending_count": gorm.Expr("pending_count + ?", len(newInputs)),
			})
	}

	return len(tasks), nil
}

// AddTransferTasksToJob adds new transfer tasks to an existing job in Redis with deduplication
func addLegacyTransferTasks(jobID int64, inputs []TransferTaskInput) (int, error) {
	if len(inputs) == 0 {
		return 0, nil
	}

	// 0. Dedup input slice in memory
	uniqueInputs := make([]TransferTaskInput, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if !seen[input.Src] {
			seen[input.Src] = true
			uniqueInputs = append(uniqueInputs, input)
		}
	}
	inputs = uniqueInputs // Use unique list

	ctx := context.Background()
	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	dedupKey := fmt.Sprintf("tx:job:%d:dedup", jobID) // Hash: Src -> 1 (or TaskID)

	// 1. Filter out duplicates from Redis
	// Pipeline exists check
	checkPipe := database.RDB.Pipeline()
	for _, input := range inputs {
		checkPipe.HExists(ctx, dedupKey, input.Src)
	}
	results, err := checkPipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	var newInputs []TransferTaskInput
	for i, res := range results {
		exists, _ := res.(*redis.BoolCmd).Result()
		if !exists {
			newInputs = append(newInputs, inputs[i])
		}
	}

	if len(newInputs) == 0 {
		return 0, nil
	}

	// 2. Determine start ID
	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()
	var startID int64
	if err != nil || len(lastIDs) == 0 {
		startID = jobID * 1000000
	} else {
		// Parse last ID
		// Redis returns string member
		var member string = lastIDs[0]
		fmt.Sscanf(member, "%d", &startID)
		startID++
	}

	var tasks []models.TransferTask
	now := time.Now()

	// 3. Prepare tasks and Pipeline Insert
	pipe := database.RDB.Pipeline()

	for i, input := range newInputs {
		task := models.TransferTask{
			ID:        startID + int64(i),
			JobID:     jobID,
			Src:       input.Src,
			Size:      input.Size,
			Status:    "PENDING",
			CreatedAt: now,
			UpdatedAt: now,
		}
		tasks = append(tasks, task)

		data, err := json.Marshal(task)
		if err != nil {
			continue
		}

		taskKey := fmt.Sprintf("tx:task:%d", task.ID)
		pipe.Set(ctx, taskKey, data, 0)
		pipe.ZAdd(ctx, jobKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})
		// Add to dedup map
		pipe.HSet(ctx, dedupKey, input.Src, task.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	// 4. 批量写入 PostgreSQL (transfer_tasks 表)
	if len(tasks) > 0 {
		for _, task := range tasks {
			result := database.DB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},
				DoNothing: true,
			}).Create(&task)
			if result.Error != nil {
				log.Printf("[AddTasks] Failed to insert task %d for job %d: %v", task.ID, jobID, result.Error)
			}
		}
	}

	var totalSizeBytes int64
	for _, input := range newInputs {
		if input.Size > 0 {
			totalSizeBytes += input.Size
		}
	}
	if totalSizeBytes > 0 {
		database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).
			UpdateColumn("total_size_bytes", gorm.Expr("total_size_bytes + ?", totalSizeBytes))
	}

	if len(newInputs) > 0 {
		database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).
			Updates(map[string]interface{}{
				"total_count":   gorm.Expr("total_count + ?", len(newInputs)),
				"pending_count": gorm.Expr("pending_count + ?", len(newInputs)),
			})
	}

	return len(tasks), nil
}

// --- Transfer Task Buffer Logic ---
func checkAndRefillTxBuffers() {
	var jobs []models.TransferJob
	// 查找状态为 Running 或 Pending 的 Job
	// Pending 状态的 Job 也需要初始化 buffer，等 Scanner 创建任务后可以立即开始传输
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusRunning, models.StatusPending}).Find(&jobs).Error; err != nil {
		log.Printf("[checkAndRefillTxBuffers] Failed to query active jobs: %v", err)
		return
	}
	log.Printf("[checkAndRefillTxBuffers] Found %d active transfer jobs", len(jobs))

	ctx := context.Background()

	for _, job := range jobs {
		jid := int64(job.JobID)
		ensureTxBuffer(jid)

		bufferMutex.RLock()
		ch, exists := txJobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			log.Printf("[checkAndRefillTxBuffers] Job %d: buffer does not exist, skipping", jid)
			continue
		}

		bufferLen := len(ch)
		key := fmt.Sprintf("tx:%d", jid)
		_, filling := fillingMap.Load(key)

		log.Printf("[checkAndRefillTxBuffers] Job %d: bufferLen=%d, pending=%d, filling=%v", jid, bufferLen, job.PendingCount, filling)

		// 检查卡死状态: Pending > 0, Buffer Empty, Not Filling
		if job.PendingCount > 0 && len(ch) == 0 {
			key := fmt.Sprintf("tx:%d", jid)
			if _, filling := fillingMap.Load(key); !filling {

				// 【修改点】根据模式检查 Offset 是否异常
				isSharded := isJobSharded(ctx, jid)
				var total int64
				var offset int64
				var offsetKey string

				if isSharded {
					// --- 新模式逻辑 ---
					offsetKey = fmt.Sprintf("tx:job:%d:offset", jid)
					// 获取 Max ID 作为 Total 的近似值
					maxIDStr, _ := database.RDB.Get(ctx, fmt.Sprintf("tx:job:%d:max_id", jid)).Result()
					fmt.Sscanf(maxIDStr, "%d", &total)
				} else {
					// --- 旧模式逻辑 ---
					offsetKey = fmt.Sprintf("tx:job:%d:offset", jid)
					jobKey := fmt.Sprintf("tx:job:%d:tasks", jid)
					totalCount, _ := database.RDB.ZCard(ctx, jobKey).Result()
					total = totalCount
				}

				// 获取当前 Offset
				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
				if offsetStr != "" {
					fmt.Sscanf(offsetStr, "%d", &offset)
				}

				// 判定逻辑：如果 Offset 已经跑到了 Total (甚至超过)，但 Pending 依然 > 0
				// 说明任务正在处理中（状态还没从 PENDING 更新），不应该重置 offset
				// 重置 offset 会导致已处理的任务被重新读取，源文件可能已被删除，导致 404 错误
				if total > 0 && offset >= total {
					// 检查是否有 RUNNING 状态的任务，如果有则说明任务正在处理中
					var runningCount int64
					database.DB.Model(&models.TransferTask{}).
						Where("job_id = ? AND status = ?", jid, "RUNNING").
						Count(&runningCount)

					if runningCount > 0 {
						fmt.Printf("Job %d: offset=%d reached total=%d, but %d tasks still RUNNING, skipping offset reset\n", jid, offset, total, runningCount)
					} else {
						// 没有 RUNNING 任务但有 PENDING 任务，可能真的卡住了
						// 只在确认没有活跃任务时才重置，且只重置一次
						fmt.Printf("Resetting offset for stuck transfer job %d (Total/Max: %d, Offset: %d, Pending: %d, Running: 0)\n", jid, total, offset, job.PendingCount)
						database.RDB.Set(ctx, offsetKey, 0, 0)
					}
				}
			}
		}

		if len(ch) < BufferLowWater {
			log.Printf("[checkAndRefillTxBuffers] Job %d: bufferLen=%d < BufferLowWater=%d, triggering refill", jid, len(ch), BufferLowWater)
			triggerTxRefill(jid)
		} else {
			log.Printf("[checkAndRefillTxBuffers] Job %d: bufferLen=%d >= BufferLowWater=%d, skipping refill", jid, len(ch), BufferLowWater)
		}
	}
}

// func checkAndRefillTxBuffers() {
// 	var jobs []models.TransferJob
// 	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusRunning}).Find(&jobs).Error; err != nil {
// 		return
// 	}

// 	ctx := context.Background()

// 	for _, job := range jobs {
// 		jid := int64(job.JobID)
// 		ensureTxBuffer(jid)

// 		bufferMutex.RLock()
// 		ch, exists := txJobBuffers[jid]
// 		bufferMutex.RUnlock()

// 		if !exists {
// 			continue
// 		}

// 		// Check for stuck state (Pending > 0, Buffer Empty, Not Filling, Offset >= Total)
// 		if job.PendingCount > 0 && len(ch) == 0 {
// 			key := fmt.Sprintf("tx:%d", jid)
// 			if _, filling := fillingMap.Load(key); !filling {
// 				offsetKey := fmt.Sprintf("tx:job:%d:offset", jid)
// 				jobKey := fmt.Sprintf("tx:job:%d:tasks", jid)

// 				total, _ := database.RDB.ZCard(ctx, jobKey).Result()
// 				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
// 				var offset int64
// 				if offsetStr != "" {
// 					fmt.Sscanf(offsetStr, "%d", &offset)
// 				}

// 				if total > 0 && offset >= total {
// 					// Reset offset
// 					fmt.Printf("Resetting offset for stuck transfer job %d (Total: %d, Offset: %d, Pending: %d)\n", jid, total, offset, job.PendingCount)
// 					database.RDB.Set(ctx, offsetKey, 0, 0)
// 				}
// 			}
// 		}

// 		if len(ch) < BufferLowWater {
// 			triggerTxRefill(jid)
// 		}
// 	}
// }

func ensureTxBuffer(jobID int64) {
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	if _, ok := txJobBuffers[jobID]; !ok {
		txJobBuffers[jobID] = make(chan models.TransferTask, BufferSize)
	}
}

func triggerTxRefill(jobID int64) {
	key := fmt.Sprintf("tx:%d", jobID)
	if _, filling := fillingMap.Load(key); !filling {
		fillingMap.Store(key, true)
		go func(jid int64) {
			defer fillingMap.Delete(fmt.Sprintf("tx:%d", jid))
			fillTxJobBuffer(jid)
		}(jobID)
	}
}
func fillTxJobBuffer(jobID int64) {
	ctx := context.Background()

	if isJobSharded(ctx, jobID) {
		fillShardedTxBuffer(ctx, jobID)
	} else {
		fillLeagcyTxJobBuffer(jobID)
	}
}

// 【新逻辑】分片读取
func fillShardedTxBuffer(ctx context.Context, jobID int64) {
	log.Printf("[fillShardedTxBuffer] Starting for job %d", jobID)
	
	// 新 Offset Key (无 {})
	offsetKey := fmt.Sprintf("tx:job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var lastTaskID int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &lastTaskID)
	}
	log.Printf("[fillShardedTxBuffer] Job %d: lastTaskID=%d", jobID, lastTaskID)

	// 获取当前最大任务 ID
	maxIDKey := fmt.Sprintf("tx:job:%d:max_id", jobID)
	maxIDStr, _ := database.RDB.Get(ctx, maxIDKey).Result()
	var currentMaxID int64
	if maxIDStr != "" {
		fmt.Sscanf(maxIDStr, "%d", &currentMaxID)
	}
	log.Printf("[fillShardedTxBuffer] Job %d: currentMaxID=%d", jobID, currentMaxID)

	// 如果 offset 超过了当前最大任务 ID，说明任务还在持续写入中
	// 我们需要等待任务写入完成，或者重置 offset 到当前最大 ID - 一个批次
	// 这样可以确保我们能读取到最近写入的任务
	if lastTaskID > currentMaxID && currentMaxID > 0 {
		// offset 跑过了，重置到 currentMaxID - TxFetchBatchSize
		// 这样可以读取最近写入的一批任务
		newOffset := currentMaxID - TxFetchBatchSize
		if newOffset < 0 {
			newOffset = 0
		}
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
		lastTaskID = newOffset
	}

	// 下一个要读的 ID
	startID := lastTaskID + 1

	// 计算 Bucket
	bucketKey := getTaskBucketKey(jobID, startID)

	// 使用 ZRangeByScore 按 ID 范围读取
	// Min: startID, Max: +inf
	ids, err := database.RDB.ZRangeByScore(ctx, bucketKey, &redis.ZRangeBy{
		Min:    fmt.Sprintf("%d", startID),
		Max:    "+inf",
		Count:  int64(TxFetchBatchSize),
		Offset: 0,
	}).Result()

	log.Printf("[fillShardedTxBuffer] Job %d: bucketKey=%s, startID=%d, ids_count=%d", jobID, bucketKey, startID, len(ids))

	if err != nil {
		log.Printf("[fillShardedTxBuffer] Job %d: ZRangeByScore error: %v", jobID, err)
		return
	}

	// 如果当前 bucket 读不到数据，有可能是因为刚好跨 bucket 了
	// 尝试读下一个 bucket (简单容错)
	if len(ids) == 0 {
		nextStartID := (startID/TaskBucketSize + 1) * TaskBucketSize
		nextBucketKey := getTaskBucketKey(jobID, nextStartID)
		log.Printf("[fillShardedTxBuffer] Job %d: current bucket empty, trying next bucket %s starting at %d", jobID, nextBucketKey, nextStartID)
		ids, _ = database.RDB.ZRangeByScore(ctx, nextBucketKey, &redis.ZRangeBy{
			Min:    fmt.Sprintf("%d", nextStartID),
			Max:    "+inf",
			Count:  int64(TxFetchBatchSize),
			Offset: 0,
		}).Result()
		log.Printf("[fillShardedTxBuffer] Job %d: next bucket returned %d ids", jobID, len(ids))
	}

	if len(ids) == 0 {
		log.Printf("[fillShardedTxBuffer] Job %d: no ids found, returning", jobID)
		return
	}

	// 获取详情
	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("tx:task:%d:%s", jobID, id))
	}

	log.Printf("[fillShardedTxBuffer] Job %d: Fetching %d task details with keys pattern tx:task:%d:*, first=%s, last=%s",
		jobID, len(keys), jobID, ids[0], ids[len(ids)-1])

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		log.Printf("[fillShardedTxBuffer] Job %d: MGET error: %v", jobID, err)
		return
	}

	bufferMutex.RLock()
	ch, exists := txJobBuffers[jobID]
	bufferMutex.RUnlock()
	if !exists {
		ensureTxBuffer(jobID)
		bufferMutex.RLock()
		ch, exists = txJobBuffers[jobID]
		bufferMutex.RUnlock()
		if !exists {
			return
		}
	}

	processed := 0
	var maxID int64 = 0

	nilCount := 0
	validCount := 0

	for i, item := range jsonList {
		// 必须先更新 maxID，即使 item 是 nil 或解析失败
		var currentID int64
		fmt.Sscanf(ids[i], "%d", &currentID)
		if currentID > maxID {
			maxID = currentID
		}

		if item == nil {
			nilCount++
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.TransferTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			validCount++
			if task.Status == "PENDING" {
				select {
				case ch <- task:
					processed++
				default:
					goto FINISH
				}
			}
		}
	}

	log.Printf("[fillShardedTxBuffer] Job %d: MGET results - total=%d, nil=%d, valid=%d, processed=%d",
		jobID, len(jsonList), nilCount, validCount, processed)

FINISH:
	if processed > 0 || maxID > lastTaskID {
		if maxID > 0 {
			database.RDB.Set(ctx, offsetKey, maxID, 0)
		} else if processed > 0 {
			// 如果 maxID 没更新（所有任务都不是 PENDING），但 processed > 0，
			// 使用 lastTaskID + processed 作为新的 offset
			newOffset := lastTaskID + int64(processed)
			database.RDB.Set(ctx, offsetKey, newOffset, 0)
		}
	}
}
func fillLeagcyTxJobBuffer(jobID int64) {
	ctx := context.Background()
	lockKey := fmt.Sprintf("tx:job:%d:lock", jobID)

	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()
	if err != nil || !ok {
		return
	}
	defer database.RDB.Del(ctx, lockKey)

	offsetKey := fmt.Sprintf("tx:job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var offset int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}

	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	start := offset
	stop := offset + int64(TxFetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()
	if err != nil || len(ids) == 0 {
		return
	}

	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("tx:task:%s", id))
	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		return
	}

	bufferMutex.RLock()
	ch, exists := txJobBuffers[jobID]
	bufferMutex.RUnlock()

	if !exists {
		return
	}

	count := 0
	processed := 0
	for _, item := range jsonList {
		processed++
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.TransferTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}
			select {
			case ch <- task:
				count++
			default:
				// Buffer full, back off the 'processed' count for this item as we didn't consume it
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}
}

func AcquireTransferTasks(c *gin.Context) {
	type AcquireRequest struct {
		WorkerID string `json:"worker_id"`
		Limit    int    `json:"limit"`
	}
	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	log.Printf("[AcquireTransferTasks] Worker %s requesting %d transfer tasks", req.WorkerID, req.Limit)

	tasks := []models.TransferTask{}

	bufferMutex.RLock()
	var jobIDs []int64
	for jid := range txJobBuffers {
		jobIDs = append(jobIDs, jid)
	}
	bufferMutex.RUnlock()

	log.Printf("[AcquireTransferTasks] Available job buffers: %v", jobIDs)

	if len(jobIDs) == 0 {
		log.Printf("[AcquireTransferTasks] No job buffers available, querying database for active jobs")

		var jobs []models.TransferJob
		if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusRunning, models.StatusPending}).Find(&jobs).Error; err != nil {
			log.Printf("[AcquireTransferTasks] Failed to query active transfer jobs: %v", err)
			c.JSON(http.StatusOK, tasks)
			return
		}

		log.Printf("[AcquireTransferTasks] Found %d active transfer jobs in database", len(jobs))

		for _, job := range jobs {
			jid := int64(job.JobID)
			ensureTxBuffer(jid)
			log.Printf("[AcquireTransferTasks] Created buffer for job %d, pending=%d", jid, job.PendingCount)
			triggerTxRefill(jid)
		}

		bufferMutex.RLock()
		for jid := range txJobBuffers {
			jobIDs = append(jobIDs, jid)
		}
		bufferMutex.RUnlock()

		log.Printf("[AcquireTransferTasks] After initialization, available job buffers: %v", jobIDs)

		if len(jobIDs) == 0 {
			log.Printf("[AcquireTransferTasks] Still no job buffers available, returning 0 tasks")
			c.JSON(http.StatusOK, tasks)
			return
		}
	}

	if len(jobIDs) == 1 {
		bufferMutex.RLock()
		ch, ok := txJobBuffers[jobIDs[0]]
		bufferMutex.RUnlock()
		if ok {
		done:
			for len(tasks) < req.Limit {
				select {
				case t := <-ch:
					tasks = append(tasks, t)
				default:
					break done
				}
			}
		}
		c.JSON(http.StatusOK, tasks)
		return
	}

	maxPerJob := req.Limit / len(jobIDs)
	if maxPerJob < 1 {
		maxPerJob = 1
	}

	// 随机打乱顺序，避免固定顺序导致的饥饿
	rand.Shuffle(len(jobIDs), func(i, j int) {
		jobIDs[i], jobIDs[j] = jobIDs[j], jobIDs[i]
	})

	// 轮询调度：每个 Job 轮流拿 1 个任务，直到 limit 满或所有 buffer 都空了
	// 这样即使某些 Job 的 buffer 暂时为空，也不会影响其他 Job 的公平性
	for i := 0; i < maxPerJob && len(tasks) < req.Limit; i++ {
		for _, jid := range jobIDs {
			if len(tasks) >= req.Limit {
				break
			}

			bufferMutex.RLock()
			ch, ok := txJobBuffers[jid]
			bufferMutex.RUnlock()
			if !ok {
				// buffer 不存在，立即初始化并触发填充
				ensureTxBuffer(jid)
				triggerTxRefill(jid)
				continue
			}

			if len(ch) < BufferLowWater {
				triggerTxRefill(jid)
			}

			select {
			case t := <-ch:
				tasks = append(tasks, t)
			default:
			}
		}
	}

	bufferMutex.RLock()
	var bufferStatus []string
	for jid, ch := range txJobBuffers {
		bufferStatus = append(bufferStatus, fmt.Sprintf("job%d=%d", jid, len(ch)))
	}
	bufferMutex.RUnlock()

	log.Printf("[AcquireTransferTasks] Returning %d tasks to worker %s, buffer status: %v", len(tasks), req.WorkerID, bufferStatus)
	c.JSON(http.StatusOK, tasks)
}
func BatchUpdateTransfer(c *gin.Context) {
	var updates []models.TransferTask
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "no updates"})
		return
	}

	ctx := context.Background()
	var keys []string
	for _, u := range updates {
		keys = append(keys, fmt.Sprintf("tx:task:%d:%d", u.JobID, u.ID))
	}
	existingJSONs, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})
		return
	}
	pipe := database.RDB.Pipeline()
	successSizeIncrements := make(map[int64]int64)

	for i, u := range updates {
		var taskKey string
		taskKey = fmt.Sprintf("tx:task:%d:%d", u.JobID, u.ID)

		val := existingJSONs[i]
		if str, ok := val.(string); ok && str != "" {
			var existing models.TransferTask
			if json.Unmarshal([]byte(str), &existing) == nil {
				if existing.Status != "COMPLETED" && u.Status == "COMPLETED" && existing.Size > 0 {
					successSizeIncrements[u.JobID] += existing.Size
				}

				if u.ErrorMessage == "" {
					u.ErrorMessage = existing.ErrorMessage
				}
				if u.WorkerID == "" {
					u.WorkerID = existing.WorkerID
				}
				if u.StartedAt.IsZero() {
					u.StartedAt = existing.StartedAt
				}
				if u.CreatedAt.IsZero() {
					u.CreatedAt = existing.CreatedAt
				}
				if u.Size == 0 {
					u.Size = existing.Size
				}
				if u.Src == "" {
					u.Src = existing.Src
				}
			}
		}

		data, err := json.Marshal(u)
		if err != nil {
			continue
		}

		pipe.Set(ctx, taskKey, data, 0)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	for jobID, bytes := range successSizeIncrements {
		if bytes > 0 {
			database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).
				UpdateColumn("success_size_bytes", gorm.Expr("success_size_bytes + ?", bytes))
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func GetFailedTasksForRetry(c *gin.Context) {
	type Request struct {
		MaxRetryCount int `json:"max_retry_count"`
	}
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()
	failedTasks := []models.TransferTask{}

	jobKeys, err := database.RDB.Keys(ctx, "tx:job:*:tasks").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get job keys: " + err.Error()})
		return
	}

	if len(jobKeys) == 0 {
		c.JSON(http.StatusOK, failedTasks)
		return
	}

	var jobIDs []int64
	for _, key := range jobKeys {
		var jid int64
		n, _ := fmt.Sscanf(key, "tx:job:%d:tasks", &jid)
		if n == 1 {
			jobIDs = append(jobIDs, jid)
		}
	}

	for _, jid := range jobIDs {
		jobKey := fmt.Sprintf("tx:job:%d:tasks", jid)
		ids, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
		if err != nil || len(ids) == 0 {
			continue
		}

		var keys []string
		for _, tid := range ids {
			var taskID int64
			fmt.Sscanf(tid, "%d", &taskID)
			keys = append(keys, fmt.Sprintf("tx:task:%d:%d", jid, taskID))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			continue
		}

		for _, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.TransferTask
			if err := json.Unmarshal([]byte(str), &task); err != nil {
				continue
			}

			if task.Status == "FAILED" && task.RetryCount < req.MaxRetryCount {
				failedTasks = append(failedTasks, task)
			}
		}
	}

	c.JSON(http.StatusOK, failedTasks)
}

func ResetTransferTask(c *gin.Context) {
	type Request struct {
		TaskID int64  `json:"task_id"`
		Status string `json:"status"`
	}
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Status != "PENDING" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Only PENDING status is allowed for reset"})
		return
	}

	ctx := context.Background()

	jobKeys, err := database.RDB.Keys(ctx, "tx:job:*:tasks").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get job keys: " + err.Error()})
		return
	}

	var jobIDs []int64
	for _, key := range jobKeys {
		var jid int64
		n, _ := fmt.Sscanf(key, "tx:job:%d:tasks", &jid)
		if n == 1 {
			jobIDs = append(jobIDs, jid)
		}
	}

	var found bool
	for _, jid := range jobIDs {
		taskKey := fmt.Sprintf("tx:task:%d:%d", jid, req.TaskID)
		val, err := database.RDB.Get(ctx, taskKey).Result()
		if err != nil {
			continue
		}

		var task models.TransferTask
		if err := json.Unmarshal([]byte(val), &task); err != nil {
			continue
		}

		if task.Status != "FAILED" {
			log.Printf("Task %d is %s, cannot reset", task.ID, task.Status)
			continue
		}

		resetTaskForAutoRetry(&task, req.Status)

		data, err := json.Marshal(task)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		database.RDB.Set(ctx, taskKey, data, 0)

		database.DB.Exec("UPDATE transfer_jobs SET failed_count = failed_count - 1, pending_count = pending_count + 1 WHERE job_id = ?", jid)

		found = true
		break
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "reset"})
}

// func BatchUpdateTransfer(c *gin.Context) {
// 	var updates []models.TransferTask
// 	if err := c.ShouldBindJSON(&updates); err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
// 		return
// 	}

// 	if len(updates) == 0 {
// 		c.JSON(http.StatusOK, gin.H{"status": "no updates"})
// 		return
// 	}

// 	ctx := context.Background()
// 	pipe := database.RDB.Pipeline()

// 	for _, u := range updates {
// 		data, err := json.Marshal(u)
// 		if err != nil {
// 			continue
// 		}

// 		pipe.Set(ctx, fmt.Sprintf("tx:task:%d", u.ID), data, 0)
// 	}

// 	_, err := pipe.Exec(ctx)
// 	if err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}

// 	c.JSON(http.StatusOK, gin.H{"status": "updated"})

// }

// --- Ffmpeg Task Logic ---

func AddFfmpegTasksToJob(jobID int64, tasks []models.FfmpegTask) (int, error) {

	if len(tasks) == 0 {

		return 0, nil

	}

	ctx := context.Background()

	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)

	// Determine start ID

	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()

	var startID int64

	if err != nil || len(lastIDs) == 0 {

		startID = jobID * 1000000

	} else {

		fmt.Sscanf(lastIDs[0], "%d", &startID)

		startID++

	}

	now := time.Now()

	pipe := database.RDB.Pipeline()

	for i, task := range tasks {

		task.ID = startID + int64(i)

		task.JobID = jobID

		task.CreatedAt = now

		task.UpdatedAt = now

		data, err := json.Marshal(task)

		if err != nil {

			continue

		}

		taskKey := fmt.Sprintf("ff:task:%d", task.ID)

		pipe.Set(ctx, taskKey, data, 0)

		pipe.ZAdd(ctx, jobKey, redis.Z{

			Score: float64(task.ID),

			Member: task.ID,
		})

	}

	_, err = pipe.Exec(ctx)

	if err != nil {

		return 0, err

	}

	var totalSizeBytes int64
	for _, task := range tasks {
		if task.VideoSize > 0 {
			totalSizeBytes += task.VideoSize
		}
		if task.AudioSize > 0 {
			totalSizeBytes += task.AudioSize
		}
	}
	if totalSizeBytes > 0 {
		database.DB.Model(&models.FfmpegJob{}).Where("id = ?", jobID).
			UpdateColumn("total_size_bytes", gorm.Expr("total_size_bytes + ?", totalSizeBytes))
	}

	return len(tasks), nil

}

func checkAndRefillFfmpegBuffers() {
	var jobs []models.FfmpegJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	ctx := context.Background()

	for _, job := range jobs {
		jid := int64(job.ID)
		ensureFfmpegBuffer(jid)

		bufferMutex.RLock()
		ch, exists := ffmpegJobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			continue
		}

		if job.PendingCount > 0 && len(ch) == 0 {
			key := fmt.Sprintf("ff:%d", jid)
			if _, filling := fillingMap.Load(key); !filling {
				offsetKey := fmt.Sprintf("ff:job:%d:offset", jid)
				jobKey := fmt.Sprintf("ff:job:%d:tasks", jid)

				total, _ := database.RDB.ZCard(ctx, jobKey).Result()
				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
				var offset int64
				if offsetStr != "" {
					fmt.Sscanf(offsetStr, "%d", &offset)
				}

				if total > 0 && offset >= total {
					fmt.Printf("Resetting offset for stuck ffmpeg job %d (Total: %d, Offset: %d, Pending: %d)\n", jid, total, offset, job.PendingCount)
					database.RDB.Set(ctx, offsetKey, 0, 0)
				}
			}
		}

		if len(ch) < BufferLowWater {
			triggerFfmpegRefill(jid)
		}
	}
}

func ensureFfmpegBuffer(jobID int64) {

	bufferMutex.Lock()

	defer bufferMutex.Unlock()

	if _, ok := ffmpegJobBuffers[jobID]; !ok {

		ffmpegJobBuffers[jobID] = make(chan models.FfmpegTask, BufferSize)

	}

}

func triggerFfmpegRefill(jobID int64) {

	key := fmt.Sprintf("ff:%d", jobID)

	if _, filling := fillingMap.Load(key); !filling {

		fillingMap.Store(key, true)

		go func(jid int64) {

			defer fillingMap.Delete(fmt.Sprintf("ff:%d", jid))

			fillFfmpegJobBuffer(jid)

		}(jobID)

	}

}

func fillFfmpegJobBuffer(jobID int64) {

	ctx := context.Background()

	lockKey := fmt.Sprintf("ff:job:%d:lock", jobID)

	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()

	if err != nil || !ok {

		return

	}

	defer database.RDB.Del(ctx, lockKey)

	offsetKey := fmt.Sprintf("ff:job:%d:offset", jobID)

	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()

	var offset int64

	if offsetStr != "" {

		fmt.Sscanf(offsetStr, "%d", &offset)

	}

	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)

	start := offset

	stop := offset + int64(FetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()

	if err != nil || len(ids) == 0 {

		return

	}

	var keys []string

	for _, id := range ids {

		keys = append(keys, fmt.Sprintf("ff:task:%s", id))

	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()

	if err != nil {

		return

	}

	bufferMutex.RLock()

	ch, exists := ffmpegJobBuffers[jobID]

	bufferMutex.RUnlock()

	if !exists {

		return

	}

	count := 0
	processed := 0

	for _, item := range jsonList {
		processed++

		if item == nil {
			continue
		}

		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.FfmpegTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}

			select {
			case ch <- task:
				count++
			default:
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}

}

func AcquireFfmpegTasks(c *gin.Context) {

	type AcquireRequest struct {
		WorkerID string `json:"worker_id"`

		Limit int `json:"limit"`
	}

	var req AcquireRequest

	if err := c.ShouldBindJSON(&req); err != nil {

		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	if req.Limit <= 0 {
		req.Limit = 1
	}

	tasks := []models.FfmpegTask{}

	bufferMutex.RLock()

	var jobIDs []int64

	for jid := range ffmpegJobBuffers {

		jobIDs = append(jobIDs, jid)

	}

	bufferMutex.RUnlock()

	if len(jobIDs) == 0 {

		c.JSON(http.StatusOK, tasks)

		return

	}

	for _, jid := range jobIDs {

		if len(tasks) >= req.Limit {
			break
		}

		bufferMutex.RLock()

		ch, ok := ffmpegJobBuffers[jid]

		bufferMutex.RUnlock()

		if !ok {
			continue
		}

		if len(ch) < BufferLowWater {

			triggerFfmpegRefill(jid)

		}

	loop:

		for len(tasks) >= req.Limit {

			select {

			case t := <-ch:

				tasks = append(tasks, t)

			default:

				break loop

			}

		}

	}
}

func StartTransferJobMonitor() {
	go func() {

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			updateCompletedTransferJobs()
		}
	}()
}

func updateCompletedTransferJobs() {
	query := `
		UPDATE transfer_jobs
		SET
			status = ?,
			end_time = NOW(),
			duration_seconds = CASE
				WHEN start_time IS NOT NULL THEN
					EXTRACT(EPOCH FROM (NOW() - start_time))::int
				ELSE
					EXTRACT(EPOCH FROM (NOW() - created_at))::int
			END
		WHERE
			status = ?
			AND total_count > 0
			AND periodic_interval = 0
			AND last_scan_time IS NOT NULL
			AND EXISTS (
				SELECT 1 FROM transfer_tasks WHERE job_id = transfer_jobs.job_id
			)
			AND job_id NOT IN (
				SELECT DISTINCT job_id FROM transfer_tasks
				WHERE status IN ('PENDING', 'RUNNING')
			)
	`
	result := database.DB.Exec(query, models.StatusCompleted, models.StatusRunning)
	if result.Error != nil {
		log.Printf("Error updating completed transfer jobs: %v", result.Error)
	} else if result.RowsAffected > 0 {
		log.Printf("Automatically completed %d transfer jobs", result.RowsAffected)
	}
}

func BatchUpdateFfmpeg(c *gin.Context) {

	var updates []models.FfmpegTask

	if err := c.ShouldBindJSON(&updates); err != nil {

		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	if len(updates) == 0 {

		c.JSON(http.StatusOK, gin.H{"status": "no updates"})

		return

	}

	ctx := context.Background()

	// 1. Fetch existing tasks to compare status

	var keys []string

	for _, u := range updates {

		keys = append(keys, fmt.Sprintf("ff:task:%d", u.ID))

	}

	existingJSONs, err := database.RDB.MGet(ctx, keys...).Result()

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})

		return

	}

	pipe := database.RDB.Pipeline()
	successSizeIncrements := make(map[int64]int64)

	for i, u := range updates {

		val := existingJSONs[i]

		if val == nil {
			continue
		}

		str, ok := val.(string)

		if !ok {
			continue
		}

		var existing models.FfmpegTask

		if err := json.Unmarshal([]byte(str), &existing); err != nil {

			continue

		}

		oldStatus := existing.Status

		newStatus := u.Status
		if oldStatus != "COMPLETED" && newStatus == "COMPLETED" {
			var sizeBytes int64
			if existing.VideoSize > 0 {
				sizeBytes += existing.VideoSize
			}
			if existing.AudioSize > 0 {
				sizeBytes += existing.AudioSize
			}
			if sizeBytes > 0 {
				successSizeIncrements[existing.JobID] += sizeBytes
			}
		}

		// Update fields

		existing.Status = newStatus

		existing.UpdatedAt = time.Now()

		if newStatus == "RUNNING" && existing.StartedAt.IsZero() {

			existing.StartedAt = time.Now()

		}

		if (newStatus == "COMPLETED" || newStatus == "FAILED") && existing.CompletedAt.IsZero() {

			existing.CompletedAt = time.Now()

		}

		// Save to Redis

		data, _ := json.Marshal(existing)

		pipe.Set(ctx, fmt.Sprintf("ff:task:%d", existing.ID), data, 0)

		// Sync to Postgres if status changed or progress update

		if oldStatus != newStatus || u.TotalCount > 0 || u.SuccessCount > 0 || u.FailedCount > 0 {

			jobID := existing.JobID

			// If status changed, use status logic

			// BUT if we have progress counts, use them to update counts absolutely.

			// We prioritize absolute counts if provided (from worker reporting)

			if u.TotalCount > 0 || u.SuccessCount > 0 || u.FailedCount > 0 {

				pending := u.TotalCount - (u.SuccessCount + u.FailedCount)

				if pending < 0 {
					pending = 0
				}

				query := `

	

	

	

								UPDATE ffmpeg_jobs SET 

	

	

	

									total_count = GREATEST(total_count, ?),

	

	

	

									success_count = ?, 

	

	

	

									failed_count = ?,

	

	

	

									pending_count = ?,

	

	

	

									status = ?,

	

	

	

									updated_at = NOW()

	

	

	

								WHERE id = ?

	

	

	

							`

				database.DB.Exec(query, u.TotalCount, u.SuccessCount, u.FailedCount, pending, newStatus, jobID)

			} else if oldStatus != newStatus {

				// Legacy status-based delta update (if no counts provided)

				var pendingDelta, runningDelta, successDelta, failedDelta int

				// Decrement old

				switch oldStatus {

				case "PENDING":
					pendingDelta--

				case "RUNNING":
					runningDelta--

				case "COMPLETED":
					successDelta--

				case "FAILED":
					failedDelta--

				}

				// Increment new

				switch newStatus {

				case "PENDING":
					pendingDelta++

				case "RUNNING":
					runningDelta++

				case "COMPLETED":
					successDelta++

				case "FAILED":
					failedDelta++

				}

				query := `

	

	

	

								UPDATE ffmpeg_jobs SET 

	

	

	

									pending_count = pending_count + ?, 

	

	

	

									running_count = running_count + ?, 

	

	

	

									success_count = success_count + ?, 

	

	

	

									failed_count = failed_count + ?,

	

	

	

									status = ?,

	

	

	

									updated_at = NOW()

	

	

	

								WHERE id = ?

	

	

	

							`

				database.DB.Exec(query, pendingDelta, runningDelta, successDelta, failedDelta, newStatus, jobID)

			}

		}

	}

	_, err = pipe.Exec(ctx)

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	for jobID, bytes := range successSizeIncrements {
		if bytes > 0 {
			database.DB.Model(&models.FfmpegJob{}).Where("id = ?", jobID).
				UpdateColumn("success_size_bytes", gorm.Expr("success_size_bytes + ?", bytes))
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})

}

// unmarshalYoutubeTaskWithFlexibleTime unmarshals a YoutubeTask with flexible time parsing
// Handles time strings that may not have timezone information
func unmarshalYoutubeTaskWithFlexibleTime(data []byte) (models.YoutubeTask, error) {
	// First, unmarshal into a map to handle time fields manually
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return models.YoutubeTask{}, err
	}

	// Parse time fields with flexible format
	parseTime := func(val interface{}) time.Time {
		if val == nil {
			return time.Time{}
		}
		str, ok := val.(string)
		if !ok {
			return time.Time{}
		}
		if str == "" || str == "0001-01-01T00:00:00Z" {
			return time.Time{}
		}

		// Try multiple time formats (order matters - try more specific first)
		formats := []string{
			time.RFC3339Nano,                // 2006-01-02T15:04:05.999999999Z07:00 (9 digits + timezone)
			time.RFC3339,                    // 2006-01-02T15:04:05Z07:00 (with timezone)
			"2006-01-02T15:04:05.999999999", // Without timezone, 9 digits (nanoseconds)
			"2006-01-02T15:04:05.999999",    // Without timezone, 6 digits (microseconds) - matches "154895"
			"2006-01-02T15:04:05.999",       // Without timezone, 3 digits (milliseconds)
			"2006-01-02T15:04:05",           // Without timezone, no microseconds
			"2006-01-02 15:04:05.999999999", // Space separator, 9 digits
			"2006-01-02 15:04:05.999999",    // Space separator, 6 digits
			"2006-01-02 15:04:05.999",       // Space separator, 3 digits
			"2006-01-02 15:04:05",           // Space separator, no microseconds
		}

		for _, format := range formats {
			if t, err := time.Parse(format, str); err == nil {
				// If no timezone in format, assume UTC
				if format[len(format)-1] != '0' && format[len(format)-1] != 'Z' && !strings.Contains(format, "Z07:00") {
					return t.UTC()
				}
				return t
			}
		}

		// Last resort: try to parse with flexible microsecond handling
		// Handle cases like "2026-01-25T02:02:23.154895" (6 digits, no timezone)
		if matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$`, str); matched {
			// Extract the base time part and microseconds part
			parts := strings.Split(str, ".")
			if len(parts) == 2 {
				baseTime := parts[0]
				microseconds := parts[1]
				// Normalize microseconds to 6 digits
				if len(microseconds) > 6 {
					microseconds = microseconds[:6]
				} else if len(microseconds) < 6 {
					microseconds = microseconds + strings.Repeat("0", 6-len(microseconds))
				}
				normalized := baseTime + "." + microseconds
				if t, err := time.Parse("2006-01-02T15:04:05.999999", normalized); err == nil {
					return t.UTC()
				}
			}
		}

		return time.Time{}
	}

	// Build the task
	task := models.YoutubeTask{}

	// Parse basic fields
	if id, ok := raw["id"].(float64); ok {
		task.ID = int64(id)
	}
	if jobID, ok := raw["job_id"].(float64); ok {
		task.JobID = int64(jobID)
	}
	if url, ok := raw["url"].(string); ok {
		task.URL = url
	}
	if audioURL, ok := raw["audio_url"].(string); ok {
		task.AudioURL = audioURL
	}
	if audioSize, ok := raw["audio_size"].(float64); ok {
		task.AudioSize = int64(audioSize)
	}
	if videoURL, ok := raw["video_url"].(string); ok {
		task.VideoURL = videoURL
	}
	if videoSize, ok := raw["video_size"].(float64); ok {
		task.VideoSize = int64(videoSize)
	}
	if status, ok := raw["status"].(string); ok {
		task.Status = status
	}
	if title, ok := raw["title"].(string); ok {
		task.Title = title
	}
	if videoID, ok := raw["video_id"].(string); ok {
		task.VideoID = videoID
	}
	if errorMessage, ok := raw["error_message"].(string); ok {
		task.ErrorMessage = errorMessage
	}
	if workerID, ok := raw["worker_id"].(string); ok {
		task.WorkerID = workerID
	}
	if isDownloadFail, ok := raw["is_download_fail"].(bool); ok {
		task.IsDownloadFail = isDownloadFail
	}

	// Parse time fields with flexible parsing
	task.StartedAt = parseTime(raw["started_at"])
	task.CompletedAt = parseTime(raw["completed_at"])
	task.CreatedAt = parseTime(raw["created_at"])
	task.UpdatedAt = parseTime(raw["updated_at"])

	return task, nil
}
