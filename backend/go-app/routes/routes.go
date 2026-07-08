package routes

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"unbound-future-backend/handlers"
	"unbound-future-backend/metrics"
)

func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		
		metrics.HttpReqDuration.WithLabelValues(c.Request.Method, c.FullPath(), status).Observe(duration)
		metrics.HttpReqTotal.WithLabelValues(c.Request.Method, c.FullPath(), status).Inc()
	}
}

func SetupRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(PrometheusMiddleware())

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := r.Group("/api")
	{
		// Auth
		auth := api.Group("/auth")
		{
			auth.GET("/feishu/login_url", handlers.GetFeishuLoginURL)
			auth.GET("/feishu/callback", handlers.FeishuCallback)
			auth.POST("/passcode/login", handlers.PasscodeLogin)
		}

        // Transfer Jobs
		jobs := api.Group("/jobs")
		{
			jobs.POST("/", handlers.CreateTransferJob)
			jobs.GET("/", handlers.ListTransferJobs)
			jobs.GET("/stats", handlers.GetTransferStats)
			jobs.GET("/pending", handlers.ListPendingTransferJobs)
			jobs.GET("/:id", handlers.GetTransferJob)
			jobs.POST("/:id/start", handlers.StartTransferJob)
			jobs.POST("/:id/stop", handlers.StopTransferJob)
			jobs.POST("/:id/retry", handlers.RetryFailedTransferTasks)
			jobs.PATCH("/:id/status", handlers.UpdateTransferJobStatus)
			jobs.POST("/:id/tasks", handlers.AddTasksToTransferJob)
			jobs.DELETE("/:id", handlers.DeleteTransferJob)
		}

        // Youtube Jobs
        ytJobs := api.Group("/youtube-jobs")
        {
            ytJobs.POST("/", handlers.CreateYoutubeJob)
            ytJobs.GET("/", handlers.ListYoutubeJobs)
            ytJobs.GET("/queue-stats", handlers.GetYoutubeQueueStats)
            ytJobs.GET("/:id", handlers.GetYoutubeJob)
            ytJobs.POST("/:id/tasks", handlers.AddTasksToYoutubeJob)
            ytJobs.POST("/:id/retry", handlers.RetryFailedYoutubeTasks)
            ytJobs.POST("/:id/retry-non-completed", handlers.RetryNonCompletedYoutubeTasks)
            ytJobs.POST("/:id/reset-offset", handlers.ResetYoutubeJobOffset)
            ytJobs.DELETE("/pending", handlers.DeletePendingYoutubeJobs)
            ytJobs.DELETE("/:id", handlers.DeleteYoutubeJob)
        }
        
        // Metadata
        meta := api.Group("/metadata")
        {
            meta.POST("/", handlers.CreateMetadata)
            meta.GET("/", handlers.ListMetadata)
            meta.GET("/:id", handlers.GetMetadata)
            meta.PUT("/:id", handlers.UpdateMetadata)
            meta.DELETE("/:id", handlers.DeleteMetadata)
        }
        
        // Tasks (New Batch Interface)
        tasks := api.Group("/tasks")
        {
            tasks.POST("/insert", handlers.BatchInsert)
            tasks.POST("/update", handlers.BatchUpdate)
            tasks.POST("/fetch", handlers.BatchFetch)
            tasks.POST("/acquire", handlers.AcquireTasks)
            tasks.POST("/delete", handlers.BatchDelete)
        }

        // Transfer Tasks
        txTasks := api.Group("/transfer-tasks")
        {
            txTasks.POST("/acquire", handlers.AcquireTransferTasks)
            txTasks.POST("/update", handlers.BatchUpdateTransfer)
            txTasks.POST("/failed-for-retry", handlers.GetFailedTasksForRetry)
            txTasks.POST("/reset", handlers.ResetTransferTask)
        }

        // Ffmpeg Jobs
        ffJobs := api.Group("/ffmpeg-jobs")
        {
            ffJobs.POST("/", handlers.CreateFfmpegJob)
            ffJobs.GET("/", handlers.ListFfmpegJobs)
            ffJobs.GET("/pending", handlers.ListPendingFfmpegJobs)
            ffJobs.GET("/:id", handlers.GetFfmpegJob)
            ffJobs.PATCH("/:id/status", handlers.UpdateFfmpegJobStatus)
            ffJobs.DELETE("/:id", handlers.DeleteFfmpegJob)
        }
        
        // Ffmpeg Tasks
        ffTasks := api.Group("/ffmpeg-tasks")
        {
            ffTasks.POST("/acquire", handlers.AcquireFfmpegTasks)
            ffTasks.POST("/update", handlers.BatchUpdateFfmpeg)
        }

        // Pipeline Jobs
        pipelines := api.Group("/pipelines")
        {
            pipelines.POST("/", handlers.CreatePipelineJob)
            pipelines.GET("/", handlers.ListPipelineJobs)
            pipelines.GET("/:id", handlers.GetPipelineJob)
            pipelines.POST("/:id/retry", handlers.RetryPipelineJob)
        }

        // Worker Cookie Configs
        cookieConfigs := api.Group("/worker-cookie-configs")
        {
            cookieConfigs.GET("", handlers.GetWorkerCookieConfig)
            cookieConfigs.GET("/machine-names", handlers.ListWorkerMachineNames)
        }

        // Youtube Tasks (Database Records)
        youtubeTasks := api.Group("/youtube-tasks")
        {
            youtubeTasks.POST("/update", handlers.UpdateYoutubeTaskRecord)
        }
	}

	return r
}
