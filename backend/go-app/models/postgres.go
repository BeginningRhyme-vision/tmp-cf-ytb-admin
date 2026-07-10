package models

import (
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusPaused    JobStatus = "PAUSED"
	StatusStopped   JobStatus = "STOPPED"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusScanning  JobStatus = "SCANNING"
)

type User struct {
	ID           uint      `gorm:"primaryKey"`
	FeishuOpenID string    `gorm:"uniqueIndex;size:255;not null"`
	Name         string    `gorm:"size:255"`
	Email        string    `gorm:"size:255"`
	AvatarURL    string    `gorm:"size:1024"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

type TransferMetadata struct {
	ID          uint          `gorm:"primaryKey" json:"id"`
	ClientName  string        `gorm:"size:255;not null" json:"client_name"`
	Endpoint    string        `gorm:"size:1024;not null" json:"endpoint"`
	AK          string        `gorm:"size:255;not null" json:"ak"`
	SKEncrypted string        `gorm:"column:sk_encrypted;size:1024;not null" json:"sk_encrypted"`
	CreatedAt   time.Time     `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time     `gorm:"autoUpdateTime" json:"updated_at"`
	Jobs        []TransferJob `gorm:"foreignKey:MetadataID" json:"jobs"`
}

func (TransferMetadata) TableName() string {
	return "transfer_metadata"
}

type TransferJob struct {
	JobID            uint       `gorm:"primaryKey;column:job_id" json:"job_id"`
	MetadataID       uint       `gorm:"index" json:"metadata_id"`
	SrcDir           string     `gorm:"size:1024;not null" json:"src_dir"`
	DstDir           string     `gorm:"size:1024;not null" json:"dst_dir"`
	Include          string     `gorm:"size:1024" json:"include"`
	Exclude          string     `gorm:"size:1024" json:"exclude"`
	DeleteSource     bool       `gorm:"default:false" json:"delete_source"`
	IsIncremental    bool       `gorm:"default:false" json:"is_incremental"`
	PeriodicInterval int        `gorm:"default:0" json:"periodic_interval"` // In seconds. 0 = not periodic
	LastScanTime     *time.Time `json:"last_scan_time"`
	LastScannedKey   *string    `gorm:"size:1024" json:"last_scanned_key"` // Last scanned key for StartAfter optimization
	Status           JobStatus  `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	StartTime        *time.Time `json:"start_time"`
	EndTime          *time.Time `json:"end_time"`
	DurationSeconds  int        `json:"duration_seconds"`
	ExecutionCount   int        `json:"execution_count"`
	TotalCount       int        `gorm:"default:0" json:"total_count"`
	PendingCount     int        `gorm:"default:0" json:"pending_count"`
	RunningCount     int        `gorm:"default:0" json:"running_count"`
	SuccessCount     int        `gorm:"default:0" json:"success_count"`
	FailedCount      int        `gorm:"default:0" json:"failed_count"`
	TotalSizeBytes   int64      `gorm:"default:0" json:"total_size_bytes"`
	SuccessSizeBytes int64      `gorm:"default:0" json:"success_size_bytes"`
	ResultMessage    string     `gorm:"type:text" json:"result_message"`
	RedisCleaned     bool       `gorm:"default:false" json:"redis_cleaned"`
	CreatedAt        time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time  `gorm:"autoUpdateTime" json:"updated_at"`

	Metadata TransferMetadata `gorm:"foreignKey:MetadataID" json:"metadata"`
}

func (TransferJob) TableName() string {
	return "transfer_jobs"
}

type YoutubeJob struct {
	ID                     uint      `gorm:"primaryKey" json:"id"`
	R2Prefix               string    `gorm:"size:1024;not null" json:"r2_prefix"`
	DownloadMode           string    `gorm:"type:varchar(20);default:'both'" json:"download_mode"` // 'both', 'audio', 'video'
	VideoSelectionStrategy string    `gorm:"type:varchar(50);default:'highest_quality'" json:"video_selection_strategy"`
	MachineName            string    `gorm:"size:255;index" json:"machine_name"` // 绑定的主机名，为空表示所有主机都可以处理
	Status                 JobStatus `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	TotalCount             int       `gorm:"default:0" json:"total_count"`
	PendingCount           int       `gorm:"default:0" json:"pending_count"`
	RunningCount           int       `gorm:"default:0" json:"running_count"`
	SuccessCount           int       `gorm:"default:0" json:"success_count"`
	FailedCount            int       `gorm:"default:0" json:"failed_count"`
	TotalSizeBytes         int64     `gorm:"default:0" json:"total_size_bytes"`
	SuccessSizeBytes       int64     `gorm:"default:0" json:"success_size_bytes"`
	CreatedAt              time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (YoutubeJob) TableName() string {
	return "youtube_jobs"
}

type FfmpegJob struct {
	ID               uint       `gorm:"primaryKey" json:"id"`
	MetadataID       uint       `gorm:"index" json:"metadata_id"`
	S3Prefix         string     `gorm:"size:1024;not null" json:"s3_prefix"` // e.g., "s3://bucket/path/"
	S3UploadPrefix   string     `gorm:"size:1024" json:"s3_upload_prefix"`   // e.g., "s3://bucket/upload_path/"
	IsIncremental    bool       `gorm:"default:false" json:"is_incremental"`
	PeriodicInterval int        `gorm:"default:0" json:"periodic_interval"` // In seconds
	LastScanTime     *time.Time `json:"last_scan_time"`
	LastScannedKey   *string    `gorm:"size:1024" json:"last_scanned_key"` // Last scanned key for StartAfter optimization
	Status           JobStatus  `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	TotalCount       int        `gorm:"default:0" json:"total_count"`
	PendingCount     int        `gorm:"default:0" json:"pending_count"`
	RunningCount     int        `gorm:"default:0" json:"running_count"`
	SuccessCount     int        `gorm:"default:0" json:"success_count"`
	FailedCount      int        `gorm:"default:0" json:"failed_count"`
	TotalSizeBytes   int64      `gorm:"default:0" json:"total_size_bytes"`
	SuccessSizeBytes int64      `gorm:"default:0" json:"success_size_bytes"`
	CreatedAt        time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time  `gorm:"autoUpdateTime" json:"updated_at"`

	Metadata TransferMetadata `gorm:"foreignKey:MetadataID" json:"metadata"`
}

func (FfmpegJob) TableName() string {
	return "ffmpeg_jobs"
}

type PipelineJob struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	Name          string    `gorm:"size:255" json:"name"`
	Status        JobStatus `gorm:"default:'RUNNING'" json:"status"`
	YoutubeJobID  uint      `json:"youtube_job_id"`
	TransferJobID uint      `json:"transfer_job_id"`
	FfmpegJobID   uint      `json:"ffmpeg_job_id"`
	YoutubeURLs   string    `gorm:"type:text" json:"youtube_urls"`
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PipelineJob) TableName() string {
	return "pipeline_jobs"
}

// WorkerCookieConfig 记录机器名和cookie的绑定关系
type WorkerCookieConfig struct {
	ID                uint      `gorm:"primaryKey" json:"id"`
	MachineName       string    `gorm:"size:255;not null" json:"machine_name"` // 机器名（允许多条记录，用于轮询多个 cookie）
	CookieContent     string    `gorm:"type:text;not null" json:"cookie_content"`          // 完整的cookie内容（text格式）
	Enabled           bool      `gorm:"default:true" json:"enabled"`                        // 启用状态
	ParseRateLimit    float64   `gorm:"default:0" json:"parse_rate_limit"`                 // 解析限流阈值速度（单位：requests/min，每分钟请求数）
	DownloadRateLimit float64   `gorm:"default:0" json:"download_rate_limit"`              // 下载限流阈值速度（单位：MB/s，每秒兆字节数）
	CreatedAt         time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (WorkerCookieConfig) TableName() string {
	return "worker_cookie_configs"
}

// YoutubeTaskRecord 记录 YouTube 任务的详细信息（数据库表）
type YoutubeTaskRecord struct {
	ID            uint       `gorm:"primaryKey" json:"id"`                                               // 任务 ID，与 JobID 组成唯一索引
	JobID         uint       `gorm:"index;not null" json:"job_id"`                                         // 关联的 YouTube Job ID
	Status        string     `gorm:"type:varchar(50);default:'PENDING'" json:"status"`                   // PENDING, RUNNING, METADATA_FETCHED, COMPLETED, FAILED
	WorkerID      string     `gorm:"size:255" json:"worker_id"`                                          // 处理该任务的 worker ID
	URL           string     `gorm:"type:text" json:"url"`                                               // 原始的 YouTube URL
	Title         string     `gorm:"type:text" json:"title"`                                             // 视频标题
	VideoID       string     `gorm:"size:255;index" json:"video_id"`                                      // YouTube 视频 ID
	AudioURL      string     `gorm:"type:text" json:"audio_url"`                                         // 音频下载 URL
	AudioSize     int64      `gorm:"default:0" json:"audio_size"`                                       // 音频文件大小（字节）
	VideoURL      string     `gorm:"type:text" json:"video_url"`                                         // 视频下载 URL
	VideoSize     int64      `gorm:"default:0" json:"video_size"`                                        // 视频文件大小（字节）
	ErrorMessage  string     `gorm:"type:text" json:"error_message"`                                      // 失败原因（如果失败）
	CreatedAt     time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
	
	// 关联的 Job
	Job YoutubeJob `gorm:"foreignKey:JobID" json:"job,omitempty"`
}

func (YoutubeTaskRecord) TableName() string {
	return "youtube_task_records" // 使用更明确的表名，避免与可能的 youtube_tasks 表冲突
}
