package database

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"unbound-future-backend/config"
	"unbound-future-backend/models"
)

var DB *gorm.DB

func InitPostgres(cfg *config.Config) error {
	var err error
	dsn := cfg.Database.URL

	// Log connection attempt (mask password for security)
	dsnForLog := dsn
	if strings.Contains(dsn, "@") {
		parts := strings.Split(dsn, "@")
		if len(parts) == 2 {
			// Mask password
			if strings.Contains(parts[0], ":") {
				userPass := strings.Split(parts[0], ":")
				if len(userPass) == 2 {
					dsnForLog = fmt.Sprintf("%s:***@%s", userPass[0], parts[1])
				}
			}
		}
	}
	log.Printf("Attempting to connect to PostgreSQL: %s", dsnForLog)
	
	// Add connect_timeout to DSN if not present (in seconds)
	if !strings.Contains(dsn, "connect_timeout") {
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		dsn = dsn + separator + "connect_timeout=10"
	}
	
	// Use context with timeout for connection
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	// Create a channel to signal completion
	done := make(chan error, 1)
	
	go func() {
		var dbErr error
		DB, dbErr = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // 改为 Info 级别以便看到迁移日志
		})
		done <- dbErr
	}()
	
	select {
	case err = <-done:
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("postgres connection timeout after 15 seconds - check if database is reachable at the configured address")
	}

	// Auto migrate
	log.Println("Starting database migration...")
	err = DB.AutoMigrate(
		&models.User{},
		&models.TransferMetadata{},
		&models.TransferJob{},
		&models.TransferTask{},
		&models.YoutubeJob{},
		&models.FfmpegJob{},
		&models.PipelineJob{},
		&models.WorkerCookieConfig{},
		&models.YoutubeTaskRecord{},
	)
	if err != nil {
		log.Printf("ERROR: AutoMigrate failed: %v", err)
		return fmt.Errorf("failed to migrate postgres schema: %w", err)
	}
	log.Println("AutoMigrate completed successfully")

	// 验证表是否创建成功
	tableName := (&models.YoutubeTaskRecord{}).TableName()
	log.Printf("Checking if table '%s' exists...", tableName)
	if !DB.Migrator().HasTable(&models.YoutubeTaskRecord{}) {
		log.Printf("Warning: Table '%s' was not created by AutoMigrate, attempting manual creation...", tableName)
		// 尝试手动创建表
		if err := DB.Migrator().CreateTable(&models.YoutubeTaskRecord{}); err != nil {
			log.Printf("Error: Failed to create table '%s' manually: %v", tableName, err)
			// 尝试直接执行 SQL
			createTableSQL := `
			CREATE TABLE IF NOT EXISTS youtube_task_records (
				id BIGSERIAL PRIMARY KEY,
				job_id BIGINT NOT NULL,
				status VARCHAR(50) DEFAULT 'PENDING',
				worker_id VARCHAR(255),
				title TEXT,
				video_id VARCHAR(255),
				audio_url TEXT,
				audio_size BIGINT DEFAULT 0,
				video_url TEXT,
				video_size BIGINT DEFAULT 0,
				error_message TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_youtube_task_records_job_id ON youtube_task_records(job_id);
			CREATE INDEX IF NOT EXISTS idx_youtube_task_records_video_id ON youtube_task_records(video_id);
			`
			if sqlErr := DB.Exec(createTableSQL).Error; sqlErr != nil {
				log.Printf("Error: Failed to create table '%s' via SQL: %v", tableName, sqlErr)
			} else {
				log.Printf("Successfully created table '%s' via SQL", tableName)
			}
		} else {
			log.Printf("Successfully created table '%s' manually", tableName)
		}
	} else {
		log.Printf("Table '%s' exists", tableName)
	}

	var indexDef string
	err = DB.Raw("SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'youtube_task_records' AND indexname = 'idx_job_task'").Scan(&indexDef).Error
	if err != nil {
		log.Printf("Warning: Failed to query idx_job_task definition: %v", err)
	}
	if indexDef != "" && !strings.Contains(indexDef, "(job_id, id)") {
		if dropErr := DB.Exec("DROP INDEX IF EXISTS idx_job_task").Error; dropErr != nil {
			log.Printf("Warning: Failed to drop wrong idx_job_task index: %v", dropErr)
		} else {
			log.Printf("Dropped wrong idx_job_task index definition: %s", indexDef)
		}
	}
	err = DB.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_job_task ON youtube_task_records (job_id, id)").Error
	if err != nil {
		log.Printf("Warning: Failed to create unique index idx_job_task(job_id,id): %v", err)
	} else {
		log.Println("Ensured unique index idx_job_task on youtube_task_records (job_id, id)")
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	log.Println("Connected to PostgreSQL successfully")
	return nil
}
