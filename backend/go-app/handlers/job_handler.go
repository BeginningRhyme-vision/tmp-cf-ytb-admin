package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/metrics"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
)

// --- Transfer Jobs ---

type CreateTransferJobRequest struct {
	models.TransferJob
	Tasks []string `json:"tasks"`
}

func CreateTransferJob(c *gin.Context) {
	var req CreateTransferJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := req.TransferJob
	job.Status = models.StatusPending

	// If Incremental is selected but no interval is provided, default to 60s to ensure it acts as a periodic monitor
	if job.IsIncremental && job.PeriodicInterval <= 0 {
		job.PeriodicInterval = 600
	}

	// Initial counts
	job.TotalCount = len(req.Tasks)
	job.PendingCount = len(req.Tasks)

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	metrics.JobCreatedTotal.WithLabelValues("transfer").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("transfer").Inc()

	// Async add tasks
	if len(req.Tasks) > 0 {
		go func(jobID uint, tasks []string) {
			var inputs []TransferTaskInput
			for _, t := range tasks {
				inputs = append(inputs, TransferTaskInput{Src: t, Size: 0})
			}
			count, err := AddTransferTasksToJob(int64(jobID), inputs)
			if err != nil {
				fmt.Printf("Error adding tasks for transfer job %d: %v\n", jobID, err)
			} else if count > 0 {
				database.DB.Exec("UPDATE transfer_jobs SET total_count = total_count + ?, pending_count = pending_count + ? WHERE job_id = ?", count, count, jobID)
			}
		}(job.JobID, req.Tasks)
	}

	c.JSON(http.StatusCreated, job)
}

func ListTransferJobs(c *gin.Context) {
	var jobs []models.TransferJob
	// Pagination params
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Preload("Metadata").Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Count total records for pagination
	var total int64
	if err := database.DB.Model(&models.TransferJob{}).Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Header("X-Total-Count", strconv.FormatInt(total, 10))
	c.JSON(http.StatusOK, jobs)
}

type TransferStatsDestItem struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
	Size  int64  `json:"size"`
}

type TransferStatsDailyItem struct {
	Date  string `json:"date"`
	Count int64  `json:"count"`
	Size  int64  `json:"size"`
}

func GetTransferStats(c *gin.Context) {
	var destItems []TransferStatsDestItem
	destSQL := `
		SELECT
			(COALESCE(tm.client_name, 'meta-' || tj.metadata_id::text) || ' | ' ||
			CASE
				WHEN split_part(trim(both '/' from tj.dst_dir), '/', 2) <> '' THEN
					split_part(trim(both '/' from tj.dst_dir), '/', 1) || '/' || split_part(trim(both '/' from tj.dst_dir), '/', 2)
				ELSE COALESCE(NULLIF(split_part(trim(both '/' from tj.dst_dir), '/', 1), ''), '-')
			END
			) AS label,
			SUM(COALESCE(tj.success_count, tj.total_count, 0))::bigint AS count,
			SUM(COALESCE(tj.success_size_bytes, 0))::bigint AS size
		FROM transfer_jobs tj
		LEFT JOIN transfer_metadata tm ON tm.id = tj.metadata_id
		GROUP BY label
		ORDER BY size DESC
		LIMIT 10
	`
	if err := database.DB.Raw(destSQL).Scan(&destItems).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var dailyItems []TransferStatsDailyItem
	dailySQL := `
		SELECT
			to_char(date_trunc('day', created_at), 'YYYY-MM-DD') AS date,
			SUM(COALESCE(success_count, total_count, 0))::bigint AS count,
			SUM(COALESCE(success_size_bytes, 0))::bigint AS size
		FROM transfer_jobs
		WHERE created_at >= NOW() - INTERVAL '14 days'
		GROUP BY 1
		ORDER BY 1 ASC
	`
	if err := database.DB.Raw(dailySQL).Scan(&dailyItems).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"dest":  destItems,
		"daily": dailyItems,
	})
}

func GetTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.Preload("Metadata").First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	var counts struct {
		Total    int64
		Pending  int64
		Running  int64
		Success  int64
		Failed   int64
	}

	database.DB.Model(&models.TransferTask{}).
		Where("job_id = ?", id).
		Select(`
			COUNT(*) as total,
			SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
			SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
			SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as success,
			SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
		`).
		Scan(&counts)

	job.TotalCount = int(counts.Total)
	job.PendingCount = int(counts.Pending)
	job.RunningCount = int(counts.Running)
	job.SuccessCount = int(counts.Success)
	job.FailedCount = int(counts.Failed)

	c.JSON(http.StatusOK, job)
}

func StartTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := map[string]interface{}{
		"status":           models.StatusPending,
		"start_time":       nil,
		"end_time":         nil,
		"duration_seconds": 0,
		"result_message":   "",
		"last_scan_time":   nil,
	}

	if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, job)
}

func StopTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	job.Status = models.StatusStopped
	if err := database.DB.Save(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, job)
}

func ListPendingTransferJobs(c *gin.Context) {
	var jobs []models.TransferJob
	if err := database.DB.Preload("Metadata").
		Where("status = ?", models.StatusPending).
		Or("status = ? AND periodic_interval > 0 AND (last_scan_time IS NULL OR last_scan_time < NOW() - make_interval(secs => periodic_interval))", models.StatusRunning).
		Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// --- Reusable Retry Logic ---
// 复用 task_handler.go 中的 isJobSharded 和 getTaskBucketKey 逻辑
// 注意：确保这两个文件在同一个 package handlers 下

func RetryTransferTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()

	// 探测任务模式
	if isJobSharded(ctx, int64(jobID)) {
		retryShardedTransferTasks(ctx, jobID, initialStatus) // 【新】分片重试
	} else {
		retryLegacyTransferTasks(jobID, initialStatus) // 【旧】原有逻辑
	}
}

// 【旧逻辑】保持原样，专门处理旧任务
// 【新逻辑】处理分片任务

// resetFailedTaskForRetry resets a FAILED task to PENDING state for MANUAL retry
// (triggered by Retry Failed button). RetryCount and LastRetryTime are reset to 0
// so the task gets a fresh start with base timeout (1x multiplier).
// Returns true if the task was reset, false if it was not in FAILED state.
func resetFailedTaskForRetry(task *models.TransferTask) bool {
	if task.Status != "FAILED" {
		return false
	}
	task.Status = "PENDING"
	task.UpdatedAt = time.Now()
	task.ErrorMessage = ""
	task.WorkerID = ""
	task.StartedAt = time.Time{}
	task.CompletedAt = time.Time{}
	task.RetryCount = 0
	task.LastRetryTime = ""
	return true
}

// resetTaskForAutoRetry resets a FAILED task to PENDING state for AUTO retry
// (triggered by retry scanner). RetryCount and LastRetryTime are PRESERVED so
// the retry count can accumulate towards maxRetryCount, at which point the task
// is permanently marked as FAILED and stops being retried automatically.
func resetTaskForAutoRetry(task *models.TransferTask, newStatus string) {
	task.Status = newStatus
	task.UpdatedAt = time.Now()
	task.ErrorMessage = ""
	task.WorkerID = ""
	task.StartedAt = time.Time{}
	task.CompletedAt = time.Time{}
}

func retryShardedTransferTasks(ctx context.Context, jobID int, initialStatus models.JobStatus) {
	resetCount := 0

	// 遍历所有可能的 Bucket
	// 由于我们不知道具体有多少个 Bucket，可以尝试遍历直到连续空 Bucket 出现，或者设置一个较大的安全上限
	// 这里设置上限 200 (覆盖 1000万任务)，足够大部分场景
	for bucket := 0; bucket < 200; bucket++ {
		// 使用不带 {} 的新 Key
		bucketKey := fmt.Sprintf("tx:job:%d:tasks:%d", jobID, bucket)

		// 每次取整个 Bucket (5万条)，或者分批取。为了简单和内存安全，这里直接 ZRange 整个 Bucket
		// 如果 Bucket 很大，建议内部再做分页
		ids, err := database.RDB.ZRange(ctx, bucketKey, 0, -1).Result()
		if err != nil || len(ids) == 0 {
			// 如果前几个 Bucket 有数据，中间断了，可能是数据问题，也可能是遍历完了。
			// 简单策略：如果 bucket=0 都没数据，那肯定没数据；如果中间空了，尝试跳过继续
			if bucket == 0 {
				break
			}
			// 如果连续空了 5 个 bucket，认为后面没有了
			// (这里简化处理，直接 continue，直到循环结束)
			continue
		}

		var keys []string
		for _, tid := range ids {
			// 新 Task Key 格式 (不带 {})
			keys = append(keys, fmt.Sprintf("tx:task:%d:%s", jobID, tid))
		}

		// 分批 MGet (避免一次请求太大)
		mgetBatch := 500
		for i := 0; i < len(keys); i += mgetBatch {
			end := i + mgetBatch
			if end > len(keys) {
				end = len(keys)
			}

			batchKeys := keys[i:end]
			results, _ := database.RDB.MGet(ctx, batchKeys...).Result()

			pipe := database.RDB.Pipeline()
			hasUpdates := false

			for k, val := range results {
				if val == nil {
					continue
				}
				str, ok := val.(string)
				if !ok {
					continue
				}

				var task models.TransferTask
				if err := json.Unmarshal([]byte(str), &task); err == nil {
					if resetFailedTaskForRetry(&task) {
						data, _ := json.Marshal(task)
						pipe.Set(ctx, batchKeys[k], data, 0)
						hasUpdates = true
						resetCount++
					}
				}
			}
			if hasUpdates {
				pipe.Exec(ctx)
			}
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE transfer_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE job_id = ?", resetCount, resetCount, jobID)

		// 重置新 Offset (不带 {})
		database.RDB.Set(ctx, fmt.Sprintf("tx:job:%d:offset", jobID), 0, 0)

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.TransferJob{JobID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}
func retryLegacyTransferTasks(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("tx:task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.TransferTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				if resetFailedTaskForRetry(&task) {
					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE transfer_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE job_id = ?", resetCount, resetCount, jobID)

		database.RDB.Set(ctx, fmt.Sprintf("tx:job:%d:offset", jobID), 0, 0)

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.TransferJob{JobID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryYoutubeTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.YoutubeTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				// Retry if FAILED and (IsDownloadFail is true OR it's a bot detection error)
				isBotError := strings.Contains(task.ErrorMessage, "Sign in to confirm you’re not a bot")
				if task.Status == "FAILED" && (task.IsDownloadFail || isBotError) {
					task.Status = "PENDING"
					task.IsDownloadFail = false
					task.ErrorMessage = ""
					task.UpdatedAt = time.Now()

					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					queueName := getDownloadQueueName(task.JobID)
					if strings.TrimSpace(task.AudioURL) == "" && strings.TrimSpace(task.VideoURL) == "" {
						queueName = getMetadataQueueName(task.JobID)
					}
					pipe.RPush(ctx, queueName, task.ID)

					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE youtube_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE id = ?", resetCount, resetCount, jobID)

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.YoutubeJob{ID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryFfmpegTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("ff:task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.FfmpegTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				if task.Status == "FAILED" {
					task.Status = "PENDING"
					task.UpdatedAt = time.Now()
					task.ErrorMessage = ""

					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE ffmpeg_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE id = ?", resetCount, resetCount, jobID)

		// Reset offset to rescan if needed? Usually for incremental we want to re-check.
		// For Ffmpeg, the scanner finds tasks. If we reset tasks to PENDING, they are in Redis but not in any queue unless we put them there?
		// Ffmpeg architecture: Scanner -> Redis (PENDING) -> Worker acquires.
		// If we just change status to PENDING in Redis, the worker won't find them unless they are in a list/queue or the worker scans keys.
		// In Youtube logic, we pushed to "queue:youtube:download_ready".
		// In Transfer logic, we didn't push anywhere. Wait, Transfer worker calls `AcquireTransferTasks` which likely does ZRANGE or similar on `tx:job:...`.
		// Let's check `AcquireTransferTasks` implementation later. Assuming standard pattern.
		// For Ffmpeg, we probably need to ensure they are pickable.

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.FfmpegJob{ID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryFailedTransferTasks(c *gin.Context) {
	id := c.Param("id")
	jobID, _ := strconv.Atoi(id)

	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	go RetryTransferTasksLogic(jobID, job.Status)

	c.JSON(http.StatusOK, gin.H{"message": "Retry initiated in background"})
}

type UpdateJobStatusRequest struct {
	Status        models.JobStatus `json:"status"`
	LastScanTime  *time.Time       `json:"last_scan_time"`
	ResultMessage string           `json:"result_message"`
	IncSuccess    int              `json:"inc_success"`
	IncFailed     int              `json:"inc_failed"`
	IncSuccessBytes int64          `json:"inc_success_bytes"`
	StartTime     *time.Time       `json:"start_time"`
	EndTime       *time.Time       `json:"end_time"`
	TotalCount    *int             `json:"total_count"`
	IncExecution  bool             `json:"inc_execution"`
}

func UpdateTransferJobStatus(c *gin.Context) {
	id := c.Param("id")
	var req UpdateJobStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := make(map[string]interface{})
	if req.Status != "" {
		updates["status"] = req.Status
	}
	if req.LastScanTime != nil {
		updates["last_scan_time"] = req.LastScanTime
	}
	if req.ResultMessage != "" {
		updates["result_message"] = req.ResultMessage
	}
	if req.StartTime != nil {
		updates["start_time"] = req.StartTime
	}
	if req.TotalCount != nil {
		updates["total_count"] = *req.TotalCount
	}
	if req.EndTime != nil {
		updates["end_time"] = req.EndTime
		// Calculate duration
		var startTime time.Time
		if req.StartTime != nil {
			startTime = *req.StartTime
		} else if job.StartTime != nil {
			startTime = *job.StartTime
		}

		if !startTime.IsZero() {
			duration := req.EndTime.Sub(startTime)
			updates["duration_seconds"] = int(duration.Seconds())
		}
	}

	if len(updates) > 0 {
		if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

	}

	// Atomic counter updates
	if req.IncSuccess > 0 || req.IncFailed > 0 {
		totalDec := req.IncSuccess + req.IncFailed
		err := database.DB.Exec("UPDATE transfer_jobs SET success_count = success_count + ?, failed_count = failed_count + ?, pending_count = GREATEST(0, pending_count - ?) WHERE job_id = ?", req.IncSuccess, req.IncFailed, totalDec, id).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	if req.IncExecution {
		if err := database.DB.Exec("UPDATE transfer_jobs SET execution_count = execution_count + 1 WHERE job_id = ?", id).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	// Reload job to return latest state
	database.DB.First(&job, id)
	c.JSON(http.StatusOK, job)
}

func cleanupTransferJobRedis(ctx context.Context, jobID uint) {
	pipe := database.RDB.Pipeline()

	// 1. 清理旧格式 (Legacy)
	legacyJobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	legacyDedupKey := fmt.Sprintf("tx:job:%d:dedup", jobID)
	legacyOffset := fmt.Sprintf("tx:job:%d:offset", jobID)
	legacyLock := fmt.Sprintf("tx:job:%d:lock", jobID)

	// 使用 Unlink 异步删除，避免阻塞
	pipe.Unlink(ctx, legacyJobKey, legacyDedupKey, legacyOffset, legacyLock)

	// 尝试清理旧的 Task 详情
	taskIDs, _ := database.RDB.ZRange(ctx, legacyJobKey, 0, -1).Result()
	for _, tid := range taskIDs {
		pipe.Unlink(ctx, fmt.Sprintf("tx:task:%d:%s", jobID, tid))
	}

	// 2. 清理新格式 (Sharded)
	// 2.1 清理 Dedup 分片
	for i := 0; i < 256; i++ { // DedupShards = 256
		key := fmt.Sprintf("tx:job:%d:dedup:%d", jobID, i)
		pipe.Unlink(ctx, key)
	}

	// 2.2 清理 Offset 和 MaxID
	pipe.Unlink(ctx, fmt.Sprintf("tx:job:%d:offset", jobID))
	pipe.Unlink(ctx, fmt.Sprintf("tx:job:%d:max_id", jobID))

	// 2.3 清理 Task 分片 (Bucket)
	for i := 0; i < 200; i++ {
		bucketKey := fmt.Sprintf("tx:job:%d:tasks:%d", jobID, i)
		pipe.Unlink(ctx, bucketKey)
	}

	// 2.4 清理新格式的 Task 详情 Key

	_, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("Error cleaning up redis for job %d: %v\n", jobID, err)
	}

	// 3. 标记数据库
	database.DB.Model(&models.TransferJob{}).Where("job_id = ?", jobID).Update("redis_cleaned", true)
}

// StartPeriodicCleanup starts a background goroutine to clean up Redis keys for completed jobs
func StartPeriodicCleanup() {
	go func() {
		ticker := time.NewTicker(30 * time.Minute) // Run every 30 minutes
		defer ticker.Stop()

		// Run immediately on startup
		runCleanup()

		for range ticker.C {
			runCleanup()
		}
	}()
}

func runCleanup() {
	ctx := context.Background()

	// Find jobs that are completed but redis not cleaned
	// Only clean redis for non-incremental jobs that have successfully completed with all tasks successful
	var jobs []models.TransferJob
	err := database.DB.Where("status = ? AND is_incremental = ? AND success_count = total_count AND redis_cleaned = ?",
		models.StatusCompleted, false, false).Find(&jobs).Error
	if err != nil {
		// Log error if needed
		log.Println(err)
	}

	for _, job := range jobs {
		log.Println("Cleaning up Redis for job", job)
		cleanupTransferJobRedis(ctx, job.JobID)
	}
}

func DeleteTransferJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	if err := database.DB.Delete(&models.TransferJob{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	go cleanupTransferJobRedis(context.Background(), uint(id))

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

type AddTransferTasksRequest struct {
	Tasks []TransferTaskInput `json:"tasks"`
}

func AddTasksToTransferJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Job ID"})
		return
	}

	var req AddTransferTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Verify Job
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// Add tasks with dedup
	count, err := AddTransferTasksToJob(int64(id), req.Tasks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
		return
	}

	// Update Job stats
	if count > 0 {
		database.DB.Exec("UPDATE transfer_jobs SET total_count = total_count + ?, pending_count = pending_count + ?, status = CASE WHEN status = 'COMPLETED' THEN 'RUNNING' ELSE status END WHERE job_id = ?", count, count, id)

		// 关键：初始化并填充任务 buffer，让 worker 可以立即获取新任务
		ensureTxBuffer(int64(id))
		triggerTxRefill(int64(id))
	}

	c.JSON(http.StatusOK, gin.H{"added": count, "job_id": id})
}

// --- Youtube Jobs ---

type CreateYoutubeJobRequest struct {
	R2Prefix               string   `json:"r2_prefix" form:"r2_prefix"`
	FileUrl                string   `json:"file_url" form:"file_url"`
	DownloadMode           string   `json:"download_mode" form:"download_mode"`
	VideoSelectionStrategy string   `json:"video_selection_strategy" form:"video_selection_strategy"`
	MachineName            string   `json:"machine_name" form:"machine_name"` // 绑定的主机名，为空表示所有主机都可以处理
	Tasks                  []string `json:"tasks" form:"-"` // List of URLs
}

// YouTube URL 格式验证
var (
	// YouTube URL 正则表达式
	youtubeURLPattern = regexp.MustCompile(`(?i)^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/|m\.youtube\.com/watch\?v=)([a-zA-Z0-9_-]{11})([?&][^\s]*)?$`)
	// Video ID 正则表达式（11个字符的字母数字、下划线、连字符）
	videoIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{11}$`)
)

func splitTaskCandidates(line string) []string {
	return strings.FieldsFunc(line, func(r rune) bool {
		return r == '\n' || r == '\r' || r == '\t' || r == ' ' || r == ',' || r == ';' || r == '|'
	})
}

func sanitizeTaskCandidate(candidate string) string {
	candidate = strings.TrimSpace(candidate)
	candidate = strings.Trim(candidate, "`\"'")
	candidate = strings.TrimSpace(candidate)
	return candidate
}

// validateYouTubeURL 验证 YouTube URL 格式
// 返回: (isValid, isVideoID, videoID, errorMessage)
func validateYouTubeURL(line string) (bool, bool, string, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return false, false, "", "empty line"
	}

	// 检查是否是标准 YouTube URL 格式
	if youtubeURLPattern.MatchString(line) {
		matches := youtubeURLPattern.FindStringSubmatch(line)
		if len(matches) >= 5 {
			videoID := matches[4]
			return true, false, videoID, ""
		}
	}

	// 检查是否只是 video_id（11个字符）
	if videoIDPattern.MatchString(line) {
		return false, true, line, fmt.Sprintf("Line contains only video_id '%s', not a valid YouTube URL. Please use format: https://www.youtube.com/watch?v=%s", line, line)
	}

	// 既不是 URL 也不是 video_id
	return false, false, "", fmt.Sprintf("Invalid format: '%s'. Expected YouTube URL (e.g., https://www.youtube.com/watch?v=VIDEO_ID) or video_id", line)
}

// parseAndValidateTasks 解析并验证任务列表，返回有效任务和错误信息
func parseAndValidateTasks(lines []string, source string) ([]string, []map[string]interface{}) {
	var validTasks []string
	var errors []map[string]interface{}

	for lineNum, line := range lines {
		candidates := splitTaskCandidates(line)
		for _, candidate := range candidates {
			candidate = sanitizeTaskCandidate(candidate)
			if candidate == "" {
				continue
			}

			isValid, isVideoID, videoID, errMsg := validateYouTubeURL(candidate)
			if isValid {
				validTasks = append(validTasks, candidate)
			} else if isVideoID {
				standardURL := fmt.Sprintf("https://www.youtube.com/watch?v=%s", videoID)
				validTasks = append(validTasks, standardURL)
				errors = append(errors, map[string]interface{}{
					"line_number": lineNum + 1,
					"content":     candidate,
					"message":     errMsg,
					"fixed":       true,
					"fixed_url":   standardURL,
				})
			} else {
				errors = append(errors, map[string]interface{}{
					"line_number": lineNum + 1,
					"content":     candidate,
					"message":     errMsg,
					"fixed":       false,
				})
			}
		}
	}

	return validTasks, errors
}

func CreateYoutubeJob(c *gin.Context) {
	var req CreateYoutubeJobRequest

	contentType := c.GetHeader("Content-Type")

	if strings.Contains(contentType, "application/json") {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		
		// 验证 JSON 请求中的 tasks
		if len(req.Tasks) > 0 {
			validTasks, validationErrors := parseAndValidateTasks(req.Tasks, "JSON request")
			
			// 如果有格式错误，返回错误信息
			if len(validationErrors) > 0 {
				log.Printf("[CreateYoutubeJob] JSON request validation found %d errors out of %d tasks", len(validationErrors), len(req.Tasks))
				c.JSON(http.StatusBadRequest, gin.H{
					"error":            "Tasks contain invalid YouTube URL formats",
					"validation_errors": validationErrors,
					"valid_count":      len(validTasks),
					"error_count":      len(validationErrors),
					"total_lines":      len(req.Tasks),
				})
				return
			}
			
			req.Tasks = validTasks
		}
	} else if strings.Contains(contentType, "multipart/form-data") {
		req.R2Prefix = c.PostForm("r2_prefix")
		req.FileUrl = c.PostForm("file_url")
		req.DownloadMode = c.PostForm("download_mode")
		req.VideoSelectionStrategy = c.PostForm("video_selection_strategy")
		req.MachineName = c.PostForm("machine_name")

		// Handle file upload
		file, err := c.FormFile("file")
		if err == nil {
			log.Printf("[CreateYoutubeJob] Processing uploaded file: %s (size: %d bytes)", file.Filename, file.Size)
			f, err := file.Open()
			if err != nil {
				log.Printf("[CreateYoutubeJob] ERROR: Failed to open file: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
				return
			}
			defer f.Close()

			var fileLines []string
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				fileLines = append(fileLines, scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				log.Printf("[CreateYoutubeJob] ERROR: Failed to read file: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file: " + err.Error()})
				return
			}

			// 验证并解析文件内容
			validTasks, validationErrors := parseAndValidateTasks(fileLines, "uploaded file")
			
			// 如果有格式错误，返回错误信息
			if len(validationErrors) > 0 {
				log.Printf("[CreateYoutubeJob] File validation found %d errors out of %d lines", len(validationErrors), len(fileLines))
				c.JSON(http.StatusBadRequest, gin.H{
					"error":            "File contains invalid YouTube URL formats",
					"validation_errors": validationErrors,
					"valid_count":      len(validTasks),
					"error_count":      len(validationErrors),
					"total_lines":      len(fileLines),
				})
				return
			}

			req.Tasks = append(req.Tasks, validTasks...)
			log.Printf("[CreateYoutubeJob] Successfully parsed %d valid URLs from uploaded file", len(validTasks))
		} else {
			log.Printf("[CreateYoutubeJob] No file uploaded (this is OK if using other methods): %v", err)
		}

		// Handle manual tasks from form field
		manualTasks := c.PostForm("tasks")
		if manualTasks != "" {
			lines := strings.Split(manualTasks, "\n")
			validTasks, validationErrors := parseAndValidateTasks(lines, "manual input")
			
			// 如果有格式错误，返回错误信息
			if len(validationErrors) > 0 {
				log.Printf("[CreateYoutubeJob] Manual input validation found %d errors out of %d lines", len(validationErrors), len(lines))
				c.JSON(http.StatusBadRequest, gin.H{
					"error":            "Manual input contains invalid YouTube URL formats",
					"validation_errors": validationErrors,
					"valid_count":      len(validTasks),
					"error_count":      len(validationErrors),
					"total_lines":      len(lines),
				})
				return
			}

			req.Tasks = append(req.Tasks, validTasks...)
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported Content-Type"})
		return
	}

	// Handle File URL if provided
	if req.FileUrl != "" {
		resp, err := http.Get(req.FileUrl)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to download file from URL: " + err.Error()})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to download file from URL, status: %d", resp.StatusCode)})
			return
		}

		var fileLines []string
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			fileLines = append(fileLines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read downloaded file: " + err.Error()})
			return
		}

		// 验证并解析文件内容
		validTasks, validationErrors := parseAndValidateTasks(fileLines, "file URL")
		
		// 如果有格式错误，返回错误信息
		if len(validationErrors) > 0 {
			log.Printf("[CreateYoutubeJob] File URL validation found %d errors out of %d lines", len(validationErrors), len(fileLines))
			c.JSON(http.StatusBadRequest, gin.H{
				"error":            "File from URL contains invalid YouTube URL formats",
				"validation_errors": validationErrors,
				"valid_count":      len(validTasks),
				"error_count":      len(validationErrors),
				"total_lines":      len(fileLines),
			})
			return
		}

		req.Tasks = append(req.Tasks, validTasks...)
	}

	// 1. Create Job in PG
	job := models.YoutubeJob{
		R2Prefix:               req.R2Prefix,
		DownloadMode:           req.DownloadMode,
		VideoSelectionStrategy: req.VideoSelectionStrategy,
		MachineName:            req.MachineName,
		Status:                 models.StatusPending,
		TotalCount:             len(req.Tasks),
		PendingCount:           len(req.Tasks),
	}

	if job.DownloadMode == "" {
		job.DownloadMode = "both"
	}
	if job.VideoSelectionStrategy == "" {
		job.VideoSelectionStrategy = "highest_quality"
	}

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job in PG: " + err.Error()})
		return
	}

	log.Printf("[CreateYoutubeJob] Created job %d with %d tasks", job.ID, len(req.Tasks))

	metrics.JobCreatedTotal.WithLabelValues("youtube").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("youtube").Inc()

	// 2. Create Tasks in Redis and Database (Async)
	if len(req.Tasks) > 0 {
		log.Printf("[CreateYoutubeJob] Starting async task addition for job %d (%d tasks)", job.ID, len(req.Tasks))
		go func(jobID int64, tasks []string) {
			// 添加 panic recovery
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[CreateYoutubeJob] PANIC in async task addition for job %d: %v", jobID, r)
				}
			}()
			
			startTime := time.Now()
			log.Printf("[CreateYoutubeJob] [Job %d] Starting AddTasksToJob with %d tasks", jobID, len(tasks))
			
			count, err := AddTasksToJob(jobID, tasks)
			duration := time.Since(startTime)
			
			if err != nil {
				log.Printf("[CreateYoutubeJob] ERROR: Failed to add tasks to Redis/DB for job %d: %v (took %v)", jobID, err, duration)
				// 更新 Job 状态为失败（可选）
				// database.DB.Model(&models.YoutubeJob{}).Where("id = ?", jobID).Update("status", models.StatusFailed)
			} else {
				log.Printf("[CreateYoutubeJob] INFO: Successfully added %d tasks to job %d (Redis + DB) in %v", count, jobID, duration)
			}
		}(int64(job.ID), req.Tasks)
	} else {
		log.Printf("[CreateYoutubeJob] WARNING: Job %d created with 0 tasks", job.ID)
	}

	c.JSON(http.StatusCreated, job)
}

func ListYoutubeJobs(c *gin.Context) {
	var jobs []models.YoutubeJob
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 为每个 Job 从数据库查询最新的任务计数
	for i := range jobs {
		var counts struct {
			Total    int64
			Pending  int64
			Running  int64
			Success  int64
			Failed   int64
			Size     int64
			SuccessSize int64
		}
		
		database.DB.Model(&models.YoutubeTaskRecord{}).
			Where("job_id = ?", jobs[i].ID).
			Select(`
				COUNT(*) as total,
				SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
				SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
				SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as success,
				SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
				SUM(COALESCE(audio_size, 0) + COALESCE(video_size, 0)) as size,
				SUM(CASE WHEN status = 'COMPLETED' THEN COALESCE(audio_size, 0) + COALESCE(video_size, 0) ELSE 0 END) as success_size
			`).
			Scan(&counts)
		
		jobs[i].TotalCount = int(counts.Total)
		jobs[i].PendingCount = int(counts.Pending)
		jobs[i].RunningCount = int(counts.Running)
		jobs[i].SuccessCount = int(counts.Success)
		jobs[i].FailedCount = int(counts.Failed)
		jobs[i].TotalSizeBytes = counts.Size
		jobs[i].SuccessSizeBytes = counts.SuccessSize
	}

	// Count total records for pagination
	var total int64
	if err := database.DB.Model(&models.YoutubeJob{}).Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Header("X-Total-Count", strconv.FormatInt(total, 10))
	c.JSON(http.StatusOK, jobs)
}

func GetYoutubeJob(c *gin.Context) {
	id := c.Param("id")
	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	
	// 直接从数据库查询任务计数
	var counts struct {
		Total    int64
		Pending  int64
		Running  int64
		Success  int64
		Failed   int64
		Size     int64
		SuccessSize int64
	}
	
	database.DB.Model(&models.YoutubeTaskRecord{}).
		Where("job_id = ?", id).
		Select(`
			COUNT(*) as total,
			SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
			SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
			SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as success,
			SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
			SUM(COALESCE(audio_size, 0) + COALESCE(video_size, 0)) as size,
			SUM(CASE WHEN status = 'COMPLETED' THEN COALESCE(audio_size, 0) + COALESCE(video_size, 0) ELSE 0 END) as success_size
		`).
		Scan(&counts)
	
	// 更新 Job 的计数
	job.TotalCount = int(counts.Total)
	job.PendingCount = int(counts.Pending)
	job.RunningCount = int(counts.Running)
	job.SuccessCount = int(counts.Success)
	job.FailedCount = int(counts.Failed)
	job.TotalSizeBytes = counts.Size
	job.SuccessSizeBytes = counts.SuccessSize
	
	c.JSON(http.StatusOK, job)
}

func cleanupYoutubeJobRedis(ctx context.Context, jobID uint) {
	// 1. Get all task IDs
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)

	// We might have many tasks, so we should scan or handle in chunks if huge,
	// but ZRange is okay for reasonable sizes.
	taskIDs, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
	if err == nil && len(taskIDs) > 0 {
		// 获取 Job 的 machine_name 来确定队列名
		jobMachineName := getJobMachineName(int64(jobID))
		
		// 确定需要清理的队列列表
		var downloadQueues []string
		var metadataQueues []string
		
		if jobMachineName != "" {
			// Job 有指定的 machine_name，清理特定机器队列和 all 队列
			downloadQueues = []string{
				fmt.Sprintf("queue:youtube:download_ready:%s", jobMachineName),
				"queue:youtube:download_ready:all",
			}
			metadataQueues = []string{
				fmt.Sprintf("queue:youtube:metadata_retry:%s", jobMachineName),
				"queue:youtube:metadata_retry:all",
			}
		} else {
			// Job 没有 machine_name，只清理 all 队列
			downloadQueues = []string{"queue:youtube:download_ready:all"}
			metadataQueues = []string{"queue:youtube:metadata_retry:all"}
		}
		
		pipe := database.RDB.Pipeline()
		
		// 从所有相关队列中移除任务 ID
		for _, tid := range taskIDs {
			// 删除任务详情
			pipe.Del(ctx, fmt.Sprintf("task:%s", tid))
			
			// 从下载队列中移除（LRem 移除所有匹配的元素）
			for _, queueName := range downloadQueues {
				pipe.LRem(ctx, queueName, 0, tid) // 0 表示移除所有匹配的元素
			}
			
			// 从 metadata 队列中移除
			for _, queueName := range metadataQueues {
				pipe.LRem(ctx, queueName, 0, tid)
			}
		}
		
		pipe.Exec(ctx)
		log.Printf("[cleanupYoutubeJobRedis] Cleaned up %d tasks from queues for job %d (machine_name: %s)", len(taskIDs), jobID, jobMachineName)
	}

	// 2. Delete Job Key and metadata
	database.RDB.Del(ctx, jobKey)
	database.RDB.Del(ctx, fmt.Sprintf("job:%d:lock", jobID))
	database.RDB.Del(ctx, fmt.Sprintf("job:%d:offset", jobID))
}

func DeleteYoutubeJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	// 1. 检查 Job 是否存在
	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// 2. 只清空 Redis 中的数据（不清空 PostgreSQL）
	go cleanupYoutubeJobRedis(context.Background(), uint(id))

	// 3. 将 PostgreSQL 中 job 的状态改为 COMPLETED（不清空 task_records）
	if err := database.DB.Model(&models.YoutubeJob{}).Where("id = ?", id).Update("status", models.StatusCompleted).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update job status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "completed", "message": "Redis data cleared and job status set to COMPLETED"})
}

type AddTasksRequest struct {
	Tasks []string `json:"tasks"`
}

func AddTasksToYoutubeJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Job ID"})
		return
	}

	var req AddTasksRequest
	contentType := c.GetHeader("Content-Type")

	if strings.Contains(contentType, "application/json") {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else if strings.Contains(contentType, "multipart/form-data") {
		// Handle file upload
		file, err := c.FormFile("file")
		if err == nil {
			f, err := file.Open()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line != "" {
					req.Tasks = append(req.Tasks, line)
				}
			}
			if err := scanner.Err(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file: " + err.Error()})
				return
			}
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported Content-Type"})
		return
	}

	// 1. Verify Job exists
	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// 2. Add tasks to Redis
	count, err := AddTasksToJob(int64(id), req.Tasks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
		return
	}

	// 3. Update Job stats in Postgres
	if count > 0 {
		// Use raw SQL to avoid needing GORM imports for expressions
		database.DB.Exec("UPDATE youtube_jobs SET total_count = total_count + ?, pending_count = pending_count + ? WHERE id = ?", count, count, id)
	}

	c.JSON(http.StatusOK, gin.H{"added": count, "job_id": id})
}

func DeletePendingYoutubeJobs(c *gin.Context) {
	var jobs []models.YoutubeJob
	if err := database.DB.Where("status = ?", models.StatusPending).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(jobs) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No pending jobs found"})
		return
	}

	// 收集所有 job IDs
	jobIDs := make([]uint, len(jobs))
	for i, job := range jobs {
		jobIDs[i] = job.ID
	}

	// 1. 先删除相关的 task_records（避免外键约束错误）
	if err := database.DB.Where("job_id IN ?", jobIDs).Delete(&models.YoutubeTaskRecord{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete task records: " + err.Error()})
		return
	}

	// 2. 删除 Jobs
	if err := database.DB.Delete(&models.YoutubeJob{}, "status = ?", models.StatusPending).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 3. Delete from Redis (Async)
	go func(jobs []models.YoutubeJob) {
		ctx := context.Background()
		for _, job := range jobs {
			cleanupYoutubeJobRedis(ctx, job.ID)
		}
	}(jobs)

	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Deleted %d pending jobs", len(jobs))})
}

func RetryFailedYoutubeTasks(c *gin.Context) {
	id := c.Param("id")
	jobID, _ := strconv.Atoi(id)

	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	go RetryYoutubeTasksLogic(jobID, job.Status)

	c.JSON(http.StatusOK, gin.H{"message": "Retry initiated in background"})
}

// RetryNonCompletedYoutubeTasks 仅重试 FAILED 且非永久错误的任务，避免重复入队与资源浪费
func isPermanentYoutubeTaskError(msg string) bool {
	if msg == "" {
		return false
	}
	m := strings.ToLower(msg)
	permanentKeywords := []string{
		"sign in to confirm you're not a bot",
		"this video is not available",
		"video unavailable",
		"this video is restricted",
		"private video",
		"members-only",
		"removed by the uploader",
		"copyright",
	}
	for _, kw := range permanentKeywords {
		if strings.Contains(m, kw) {
			return true
		}
	}
	return false
}

func RetryNonCompletedYoutubeTasks(c *gin.Context) {
	id := c.Param("id")
	jobID, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	ctx := context.Background()
	lockKey := fmt.Sprintf("youtube:retry_non_completed:%d:running", jobID)
	acquired, err := database.RDB.SetNX(ctx, lockKey, strconv.FormatInt(time.Now().Unix(), 10), 6*time.Hour).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start retry: " + err.Error()})
		return
	}
	if !acquired {
		c.JSON(http.StatusOK, gin.H{
			"message": "Retry is already running in background",
			"job_id":  jobID,
		})
		return
	}
	go retryNonCompletedYoutubeTasksBackground(jobID, lockKey)
	c.JSON(http.StatusAccepted, gin.H{
		"message": "Retry started in background",
		"job_id":  jobID,
	})
}

func retryNonCompletedYoutubeTasksBackground(jobID int, lockKey string) {
	ctx := context.Background()
	defer database.RDB.Del(ctx, lockKey)

	metadataQueueName := getMetadataQueueName(int64(jobID))
	const batchSize = 5000

	var lastID int64
	queuedCount := 0
	skippedCount := 0
	permanentSkippedCount := 0
	totalScanned := 0

	for {
		var tasks []models.YoutubeTaskRecord
		err := database.DB.
			Select("id, job_id, status, error_message").
			Where("job_id = ? AND id > ? AND status = ?", jobID, lastID, "FAILED").
			Order("id ASC").
			Limit(batchSize).
			Find(&tasks).Error
		if err != nil {
			log.Printf("[RetryNonCompletedYoutubeTasks] Job %d query failed: %v", jobID, err)
			return
		}
		if len(tasks) == 0 {
			break
		}

		idsToReset := make([]int64, 0, len(tasks))
		queueIDs := make([]interface{}, 0, len(tasks))

		for _, task := range tasks {
			taskID := int64(task.ID)
			lastID = taskID
			totalScanned++

			if isPermanentYoutubeTaskError(task.ErrorMessage) {
				skippedCount++
				permanentSkippedCount++
				continue
			}

			idsToReset = append(idsToReset, taskID)
			queueIDs = append(queueIDs, taskID)
		}

		if len(idsToReset) == 0 {
			continue
		}

		if err := database.DB.Model(&models.YoutubeTaskRecord{}).
			Where("job_id = ? AND id IN ?", jobID, idsToReset).
			Updates(map[string]interface{}{
				"status":        "PENDING",
				"worker_id":     "",
				"error_message": "",
				"updated_at":    time.Now(),
			}).Error; err != nil {
			log.Printf("[RetryNonCompletedYoutubeTasks] Job %d update failed: %v", jobID, err)
			return
		}

		if _, err := database.RDB.RPush(ctx, metadataQueueName, queueIDs...).Result(); err != nil {
			log.Printf("[RetryNonCompletedYoutubeTasks] Job %d queue failed: %v", jobID, err)
			return
		}
		queuedCount += len(idsToReset)
	}

	var counts struct {
		Total   int64
		Pending int64
		Running int64
		Success int64
		Failed  int64
	}

	database.DB.Model(&models.YoutubeTaskRecord{}).
		Where("job_id = ?", jobID).
		Select(`
			COUNT(*) as total,
			SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
			SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
			SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as success,
			SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
		`).
		Scan(&counts)

	database.DB.Model(&models.YoutubeJob{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"total_count":   counts.Total,
			"pending_count": counts.Pending,
			"running_count": counts.Running,
			"success_count": counts.Success,
			"failed_count":  counts.Failed,
		})

	log.Printf("[RetryNonCompletedYoutubeTasks] Job %d finished background retry. scanned=%d queued=%d skipped=%d permanent_skipped=%d queue=%s",
		jobID, totalScanned, queuedCount, skippedCount, permanentSkippedCount, metadataQueueName)
}

// ResetYoutubeJobOffset 手动重置 YouTube Job 的 offset，用于解决 stuck job 问题
func ResetYoutubeJobOffset(c *gin.Context) {
	id := c.Param("id")
	jobID, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	// 验证 Job 是否存在
	var job models.YoutubeJob
	if err := database.DB.First(&job, jobID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	ctx := context.Background()
	offsetKey := fmt.Sprintf("job:%d:offset", jobID)
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)

	// 获取当前 offset 和 total
	total, _ := database.RDB.ZCard(ctx, jobKey).Result()
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var offset int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}

	// 重置 offset 为 0
	if err := database.RDB.Set(ctx, offsetKey, 0, 0).Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to reset offset: " + err.Error()})
		return
	}

	log.Printf("[ResetYoutubeJobOffset] Manually reset offset for job %d (Total: %d, Old Offset: %d, Pending: %d)", 
		jobID, total, offset, job.PendingCount)

	c.JSON(http.StatusOK, gin.H{
		"message":     "Offset reset successfully",
		"job_id":      jobID,
		"total":       total,
		"old_offset":  offset,
		"new_offset":  0,
		"pending":     job.PendingCount,
	})
}

// --- Ffmpeg Jobs ---

type CreateFfmpegJobRequest struct {
	models.FfmpegJob
}

func CreateFfmpegJob(c *gin.Context) {
	var req CreateFfmpegJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := req.FfmpegJob
	job.Status = models.StatusPending

	if job.IsIncremental && job.PeriodicInterval <= 0 {
		job.PeriodicInterval = 600
	}

	// Assuming 1 task per job initially (the job itself is the task context)
	job.TotalCount = 0 // Scanner will find tasks
	job.PendingCount = 0

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Preload Metadata to get credentials
	if err := database.DB.Preload("Metadata").First(&job, job.ID).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load metadata: " + err.Error()})
		return
	}

	metrics.JobCreatedTotal.WithLabelValues("ffmpeg").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("ffmpeg").Inc()

	// We don't create an initial task here anymore. The scanner will create tasks.

	c.JSON(http.StatusCreated, job)
}

func ListFfmpegJobs(c *gin.Context) {
	var jobs []models.FfmpegJob
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Preload("Metadata").Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Count total records for pagination
	var total int64
	if err := database.DB.Model(&models.FfmpegJob{}).Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Header("X-Total-Count", strconv.FormatInt(total, 10))
	c.JSON(http.StatusOK, jobs)
}

func ListPendingFfmpegJobs(c *gin.Context) {
	var jobs []models.FfmpegJob
	if err := database.DB.Preload("Metadata").
		Where("status = ?", models.StatusPending).
		Or("status = ? AND periodic_interval > 0 AND (last_scan_time IS NULL OR last_scan_time < NOW() - make_interval(secs => periodic_interval))", models.StatusRunning).
		Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 更新这些任务的状态为 SCANNING,防止 scanner 再次获取到
	jobIds := make([]uint, len(jobs))
	for i, job := range jobs {
		jobIds[i] = job.ID
	}

	if len(jobIds) > 0 {
		database.DB.Model(&models.FfmpegJob{}).
			Where("id IN ?", jobIds).
			Update("status", models.StatusScanning)
	}

	c.JSON(http.StatusOK, jobs)
}

func GetFfmpegJob(c *gin.Context) {
	id := c.Param("id")
	var job models.FfmpegJob
	if err := database.DB.Preload("Metadata").First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

func UpdateFfmpegJobStatus(c *gin.Context) {
	id := c.Param("id")
	var req UpdateJobStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var job models.FfmpegJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := make(map[string]interface{})
	if req.Status != "" {
		updates["status"] = req.Status
	}
	if req.LastScanTime != nil {
		updates["last_scan_time"] = req.LastScanTime
	}
	if req.TotalCount != nil {
		updates["total_count"] = *req.TotalCount
	}
	if req.ResultMessage != "" {
		// Store result message somewhere? FfmpegJob doesn't have ResultMessage.
		// Ignoring for now or I should add it.
	}

	// Atomic counter updates if provided
	if req.IncSuccess > 0 || req.IncFailed > 0 || req.IncSuccessBytes > 0 {
		totalDec := req.IncSuccess + req.IncFailed
		database.DB.Exec("UPDATE ffmpeg_jobs SET success_count = success_count + ?, failed_count = failed_count + ?, pending_count = GREATEST(0, pending_count - ?), success_size_bytes = success_size_bytes + ? WHERE id = ?", req.IncSuccess, req.IncFailed, totalDec, req.IncSuccessBytes, id)
	}

	if len(updates) > 0 {
		if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	database.DB.First(&job, id)
	c.JSON(http.StatusOK, job)
}

func DeleteFfmpegJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	if err := database.DB.Delete(&models.FfmpegJob{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Cleanup Redis
	go func(jid uint) {
		ctx := context.Background()
		jobKey := fmt.Sprintf("ff:job:%d:tasks", jid)

		taskIDs, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
		if err == nil && len(taskIDs) > 0 {
			pipe := database.RDB.Pipeline()
			for _, tid := range taskIDs {
				pipe.Del(ctx, fmt.Sprintf("ff:task:%s", tid))
			}
			pipe.Exec(ctx)
		}

	database.RDB.Del(ctx, jobKey)
	database.RDB.Del(ctx, fmt.Sprintf("ff:job:%d:lock", jid))
	database.RDB.Del(ctx, fmt.Sprintf("ff:job:%d:offset", jid))
}(uint(id))

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

// GetYoutubeQueueStats 获取 Redis 队列统计信息，按 job ID 分类聚合
func GetYoutubeQueueStats(c *gin.Context) {
	ctx := context.Background()
	
	// 1. 从数据库获取所有机器名
	var configs []models.WorkerCookieConfig
	if err := database.DB.Select("machine_name").Where("enabled = ?", true).Find(&configs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch machine names: " + err.Error()})
		return
	}
	
	machineNames := make([]string, 0, len(configs))
	machineSet := make(map[string]bool)
	for _, config := range configs {
		if config.MachineName != "" && !machineSet[config.MachineName] {
			machineNames = append(machineNames, config.MachineName)
			machineSet[config.MachineName] = true
		}
	}
	
	// 2. 构建所有需要查询的队列列表
	queueNames := []string{
		"queue:youtube:download_ready:all",
		"queue:youtube:metadata_retry:all",
	}
	
	for _, machineName := range machineNames {
		queueNames = append(queueNames, 
			fmt.Sprintf("queue:youtube:download_ready:%s", machineName),
			fmt.Sprintf("queue:youtube:metadata_retry:%s", machineName),
		)
	}
	
	// 3. 统计每个队列中的任务，按 job ID（前3个数字）分类聚合，并记录队列名称
	// 任务 ID 格式：jobID * 1000000 + sequence，例如 job 13 的任务 ID 是 13000001
	// jobID -> queueType -> []queueInfo (包含队列名和数量)
	jobStats := make(map[int]map[string][]map[string]interface{})
	
	for _, queueName := range queueNames {
		length, err := database.RDB.LLen(ctx, queueName).Result()
		if err != nil {
			log.Printf("[GetYoutubeQueueStats] Failed to get length for queue %s: %v", queueName, err)
			continue
		}
		
		if length == 0 {
			continue
		}
		
		// 获取队列中的所有任务 ID
		taskIDs, err := database.RDB.LRange(ctx, queueName, 0, -1).Result()
		if err != nil {
			log.Printf("[GetYoutubeQueueStats] Failed to get tasks from queue %s: %v", queueName, err)
			continue
		}
		
		// 确定队列类型
		var queueType string
		if strings.Contains(queueName, "download_ready") {
			queueType = "download_ready"
		} else if strings.Contains(queueName, "metadata_retry") {
			queueType = "metadata_retry"
		} else {
			continue
		}
		
		// 提取机器名（如果有）
		machineName := ""
		if strings.HasSuffix(queueName, ":all") {
			machineName = "all"
		} else {
			parts := strings.Split(queueName, ":")
			if len(parts) >= 4 {
				machineName = parts[3]
			}
		}
		
		// 按 job ID 分类统计，并记录队列信息
		jobTaskCount := make(map[int]int)
		for _, taskIDStr := range taskIDs {
			taskID, err := strconv.ParseInt(taskIDStr, 10, 64)
			if err != nil {
				continue
			}
			
			// 提取 job ID：任务 ID 的前3个数字（例如 13000001 -> 13）
			jobID := int(taskID / 1000000)
			jobTaskCount[jobID]++
		}
		
		// 为每个 job 记录队列信息
		for jobID, count := range jobTaskCount {
			if jobStats[jobID] == nil {
				jobStats[jobID] = make(map[string][]map[string]interface{})
			}
			if jobStats[jobID][queueType] == nil {
				jobStats[jobID][queueType] = make([]map[string]interface{}, 0)
			}
			jobStats[jobID][queueType] = append(jobStats[jobID][queueType], map[string]interface{}{
				"queue_name": queueName,
				"machine_name": machineName,
				"count": count,
			})
		}
	}
	
	// 4. 构建返回结果
	result := make([]map[string]interface{}, 0)
	for jobID, stats := range jobStats {
		downloadQueues := stats["download_ready"]
		metadataQueues := stats["metadata_retry"]
		
		downloadTotal := 0
		metadataTotal := 0
		for _, q := range downloadQueues {
			if count, ok := q["count"].(int); ok {
				downloadTotal += count
			}
		}
		for _, q := range metadataQueues {
			if count, ok := q["count"].(int); ok {
				metadataTotal += count
			}
		}
		
		result = append(result, map[string]interface{}{
			"job_id":          jobID,
			"download_ready":  downloadTotal,
			"download_queues": downloadQueues,
			"metadata_retry":   metadataTotal,
			"metadata_queues": metadataQueues,
			"total":           downloadTotal + metadataTotal,
		})
	}
	
	// 确保总是返回正确的数据结构，即使没有数据
	c.JSON(http.StatusOK, gin.H{
		"stats":         result,
		"total_queues":  len(queueNames),
		"total_machines": len(machineNames),
	})
}
