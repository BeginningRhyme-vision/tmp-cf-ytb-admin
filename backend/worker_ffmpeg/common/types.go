package common

import "time"

type FfmpegJob struct {
	ID               int64     `json:"id"`
	S3Prefix         string    `json:"s3_prefix"`
	S3UploadPrefix   string    `json:"s3_upload_prefix"`
	IsIncremental    bool      `json:"is_incremental"`
	PeriodicInterval int       `json:"periodic_interval"`
	LastScanTime     *time.Time `json:"last_scan_time"`
	LastScannedKey   *string   `json:"last_scanned_key"`
	Status           string    `json:"status"`
	Metadata         TransferMetadata `json:"metadata"`
}

type TransferMetadata struct {
	Endpoint    string `json:"endpoint"`
	AK          string `json:"ak"`
	SKEncrypted string `json:"sk_encrypted"`
}

type FfmpegTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	VideoKey     string    `json:"video_key"`
	AudioKey     string    `json:"audio_key"`
	VideoSize    int64     `json:"video_size"`
	AudioSize    int64     `json:"audio_size"`
	Status       string    `json:"status"`
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`

	// For retry logic
	RetryCount int `json:"retry_count"`
}

type UpdateJobStatusRequest struct {
	Status         string     `json:"status,omitempty"`
	LastScanTime   *time.Time `json:"last_scan_time,omitempty"`
	LastScannedKey *string    `json:"last_scanned_key,omitempty"`
	ResultMessage  string     `json:"result_message,omitempty"`
	TotalCount     *int       `json:"total_count,omitempty"`
	IncSuccess     int        `json:"inc_success,omitempty"`
	IncFailed      int        `json:"inc_failed,omitempty"`
	IncSuccessBytes int64    `json:"inc_success_bytes,omitempty"`
}
