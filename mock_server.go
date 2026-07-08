package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type SystemLoadConfig struct {
	CPUUsage             float64 `json:"cpu_usage"`
	MemoryUsage          float64 `json:"memory_usage"`
	DiskUsage            float64 `json:"disk_usage"`
	ActiveTransferTasks  int     `json:"active_transfer_tasks"`
	PendingTransferTasks int     `json:"pending_transfer_tasks"`
	RedisConnected       bool    `json:"redis_connected"`
	PostgresConnected    bool    `json:"postgres_connected"`
	FailRate             float64 `json:"fail_rate"`
	ResponseDelayMs      int     `json:"response_delay_ms"`
}

var (
	loadConfig   SystemLoadConfig
	loadMutex    sync.RWMutex
	taskStore    = make(map[int64]*TransferTask)
	taskMutex    sync.RWMutex
	taskIDCounter int64 = 3000
)

type SystemLoadResponse struct {
	CPUUsage             float64 `json:"cpu_usage"`
	MemoryUsage          float64 `json:"memory_usage"`
	DiskUsage            float64 `json:"disk_usage"`
	ActiveTransferTasks  int     `json:"active_transfer_tasks"`
	PendingTransferTasks int     `json:"pending_transfer_tasks"`
	RedisConnected       bool    `json:"redis_connected"`
	PostgresConnected    bool    `json:"postgres_connected"`
}

type TransferTasksStatsResponse struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Total     int `json:"total"`
	ActiveJobs int `json:"active_jobs"`
}

type TransferTask struct {
	ID            int64  `json:"id"`
	JobID         int64  `json:"job_id"`
	Src           string `json:"src"`
	Size          int64  `json:"size"`
	Status        string `json:"status"`
	RetryCount    int    `json:"retry_count"`
	LastRetryTime string `json:"last_retry_time"`
}

func init() {
	loadConfig = SystemLoadConfig{
		CPUUsage:             0.65,
		MemoryUsage:          0.72,
		DiskUsage:            0.45,
		ActiveTransferTasks:  150,
		PendingTransferTasks: 500,
		RedisConnected:       true,
		PostgresConnected:    true,
		FailRate:             0,
		ResponseDelayMs:      0,
	}

	initMockTasks()
}

func initMockTasks() {
	taskMutex.Lock()
	defer taskMutex.Unlock()

	tasks := []TransferTask{
		{ID: 2001, JobID: 100, Src: "https://example.com/failed1.mp4", Size: 1024 * 1024 * 100, Status: "FAILED", RetryCount: 1, LastRetryTime: time.Now().Add(-5 * time.Minute).Format(time.RFC3339)},
		{ID: 2002, JobID: 100, Src: "https://example.com/failed2.mp4", Size: 1024 * 1024 * 200, Status: "FAILED", RetryCount: 2, LastRetryTime: time.Now().Add(-15 * time.Minute).Format(time.RFC3339)},
		{ID: 2003, JobID: 101, Src: "https://example.com/failed3.mp4", Size: 1024 * 1024 * 500, Status: "FAILED", RetryCount: 0, LastRetryTime: ""},
		{ID: 2004, JobID: 101, Src: "https://example.com/failed4.mp4", Size: 1024 * 1024 * 1000, Status: "FAILED", RetryCount: 3, LastRetryTime: time.Now().Add(-30 * time.Minute).Format(time.RFC3339)},
		{ID: 2005, JobID: 102, Src: "https://example.com/failed5.mp4", Size: 1024 * 1024 * 50, Status: "FAILED", RetryCount: 30, LastRetryTime: time.Now().Add(-10 * time.Minute).Format(time.RFC3339)},
		{ID: 2006, JobID: 102, Src: "https://example.com/failed6.mp4", Size: 1024 * 1024 * 80, Status: "FAILED", RetryCount: 1, LastRetryTime: "invalid-time-format"},
		{ID: 2007, JobID: 102, Src: "https://example.com/failed7.mp4", Size: 1024 * 1024 * 120, Status: "FAILED", RetryCount: 1, LastRetryTime: time.Now().Add(1 * time.Hour).Format(time.RFC3339)},
		{ID: 2008, JobID: 103, Src: "https://example.com/failed8.mp4", Size: 1024 * 1024 * 60, Status: "FAILED", RetryCount: 1, LastRetryTime: time.Now().Add(-30 * time.Second).Format(time.RFC3339)},
		{ID: 2009, JobID: 103, Src: "https://example.com/failed9.mp4", Size: 1024 * 1024 * 90, Status: "RUNNING", RetryCount: 1, LastRetryTime: time.Now().Add(-5 * time.Minute).Format(time.RFC3339)},
		{ID: 2010, JobID: 103, Src: "https://example.com/failed10.mp4", Size: 1024 * 1024 * 2000, Status: "FAILED", RetryCount: 0, LastRetryTime: ""},
		{ID: 2011, JobID: 104, Src: "https://example.com/failed11.mp4", Size: 0, Status: "FAILED", RetryCount: 1, LastRetryTime: time.Now().Add(-2 * time.Minute).Format(time.RFC3339)},
		{ID: 2012, JobID: 104, Src: "https://example.com/failed12.mp4", Size: 1024 * 1024 * 500, Status: "COMPLETED", RetryCount: 2, LastRetryTime: time.Now().Add(-10 * time.Minute).Format(time.RFC3339)},
		{ID: 2013, JobID: 104, Src: "https://example.com/failed13.mp4", Size: 1024 * 1024 * 300, Status: "PENDING", RetryCount: 1, LastRetryTime: time.Now().Add(-1 * time.Minute).Format(time.RFC3339)},
	}

	for i := range tasks {
		taskStore[tasks[i].ID] = &tasks[i]
	}
}

func getLoadConfig() SystemLoadConfig {
	loadMutex.RLock()
	defer loadMutex.RUnlock()
	return loadConfig
}

func applyPreset(preset string) {
	loadMutex.Lock()
	defer loadMutex.Unlock()

	switch preset {
	case "normal":
		loadConfig = SystemLoadConfig{CPUUsage: 0.4, MemoryUsage: 0.5, DiskUsage: 0.35, ActiveTransferTasks: 100, PendingTransferTasks: 200, RedisConnected: true, PostgresConnected: true, FailRate: 0, ResponseDelayMs: 0}
	case "high":
		loadConfig = SystemLoadConfig{CPUUsage: 0.8, MemoryUsage: 0.85, DiskUsage: 0.7, ActiveTransferTasks: 500, PendingTransferTasks: 1000, RedisConnected: true, PostgresConnected: true, FailRate: 0.1, ResponseDelayMs: 100}
	case "critical":
		loadConfig = SystemLoadConfig{CPUUsage: 0.95, MemoryUsage: 0.95, DiskUsage: 0.9, ActiveTransferTasks: 1000, PendingTransferTasks: 5000, RedisConnected: true, PostgresConnected: true, FailRate: 0.3, ResponseDelayMs: 500}
	case "failure":
		loadConfig = SystemLoadConfig{CPUUsage: 0.99, MemoryUsage: 0.99, DiskUsage: 0.99, ActiveTransferTasks: 2000, PendingTransferTasks: 10000, RedisConnected: false, PostgresConnected: false, FailRate: 0.8, ResponseDelayMs: 1000}
	}
	log.Printf("[Mock] Load preset applied: %s", preset)
}

func simulateFailure() bool {
	loadMutex.RLock()
	defer loadMutex.RUnlock()
	return rand.Float64() < loadConfig.FailRate
}

func simulateDelay() {
	loadMutex.RLock()
	defer loadMutex.RUnlock()
	if loadConfig.ResponseDelayMs > 0 {
		time.Sleep(time.Duration(loadConfig.ResponseDelayMs) * time.Millisecond)
	}
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/system/load", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			log.Printf("[Mock] GET /api/system/load - FAILED (simulated)")
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")
		cfg := getLoadConfig()
		response := SystemLoadResponse{
			CPUUsage:             cfg.CPUUsage,
			MemoryUsage:          cfg.MemoryUsage,
			DiskUsage:            cfg.DiskUsage,
			ActiveTransferTasks:  cfg.ActiveTransferTasks,
			PendingTransferTasks: cfg.PendingTransferTasks,
			RedisConnected:       cfg.RedisConnected,
			PostgresConnected:    cfg.PostgresConnected,
		}
		json.NewEncoder(w).Encode(response)
		log.Printf("[Mock] GET /api/system/load - OK")
	})

	mux.HandleFunc("/api/system/set-load", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		preset := r.URL.Query().Get("preset")
		if preset != "" {
			applyPreset(preset)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status": "ok", "preset": "%s"}`, preset)
			return
		}

		var newConfig SystemLoadConfig
		if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		loadMutex.Lock()
		loadConfig = newConfig
		loadMutex.Unlock()

		log.Printf("[Mock] POST /api/system/set-load - CPU: %.2f, Memory: %.2f, FailRate: %.2f, Delay: %dms",
			newConfig.CPUUsage, newConfig.MemoryUsage, newConfig.FailRate, newConfig.ResponseDelayMs)

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "ok"}`)
	})

	mux.HandleFunc("/api/transfer-tasks/stats", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")
		cfg := getLoadConfig()

		response := TransferTasksStatsResponse{
			Pending:   cfg.PendingTransferTasks,
			Running:   cfg.ActiveTransferTasks,
			Completed: 10000,
			Failed:    len(taskStore),
			Total:     cfg.PendingTransferTasks + cfg.ActiveTransferTasks + 10000 + len(taskStore),
			ActiveJobs: 5,
		}

		json.NewEncoder(w).Encode(response)
		log.Printf("[Mock] GET /api/transfer-tasks/stats - OK")
	})

	mux.HandleFunc("/api/transfer-tasks/acquire", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")

		tasks := []TransferTask{
			{ID: 1001, JobID: 100, Src: "https://example.com/file1.mp4", Size: 1024 * 1024 * 10, Status: "PENDING", RetryCount: 0},
			{ID: 1002, JobID: 100, Src: "https://example.com/file2.mp4", Size: 1024 * 1024 * 50, Status: "PENDING", RetryCount: 0},
			{ID: 1003, JobID: 101, Src: "https://example.com/file3.mp4", Size: 1024 * 1024 * 100, Status: "PENDING", RetryCount: 1, LastRetryTime: time.Now().Add(-10 * time.Minute).Format(time.RFC3339)},
			{ID: 1004, JobID: 101, Src: "https://example.com/file4.mp4", Size: 1024 * 1024 * 1024 * 10, Status: "PENDING", RetryCount: 0},
			{ID: 1005, JobID: 102, Src: "https://example.com/file5.mp4", Size: 0, Status: "PENDING", RetryCount: 0},
		}

		json.NewEncoder(w).Encode(tasks)
		log.Printf("[Mock] POST /api/transfer-tasks/acquire - returned %d tasks", len(tasks))
	})

	mux.HandleFunc("/api/transfer-tasks/update", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")

		var tasks []TransferTask
		if err := json.NewDecoder(r.Body).Decode(&tasks); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		taskMutex.Lock()
		for _, t := range tasks {
			if existing, ok := taskStore[t.ID]; ok {
				existing.Status = t.Status
				existing.RetryCount = t.RetryCount
				existing.LastRetryTime = t.LastRetryTime
			} else {
				newTask := t
				taskStore[t.ID] = &newTask
			}
			log.Printf("[Mock] POST /api/transfer-tasks/update - Task %d: %s (retry_count: %d)", t.ID, t.Status, t.RetryCount)
		}
		taskMutex.Unlock()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"updated": %d}`, len(tasks))
	})

	mux.HandleFunc("/api/transfer-tasks/failed-for-retry", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")

		type Request struct {
			MaxRetryCount int `json:"max_retry_count"`
		}
		var req Request
		json.NewDecoder(r.Body).Decode(&req)

		log.Printf("[Mock] POST /api/transfer-tasks/failed-for-retry - max_retry_count: %d", req.MaxRetryCount)

		taskMutex.RLock()
		var failedTasks []TransferTask
		for _, task := range taskStore {
			if task.Status == "FAILED" && task.RetryCount < req.MaxRetryCount {
				failedTasks = append(failedTasks, *task)
			}
		}
		taskMutex.RUnlock()

		json.NewEncoder(w).Encode(failedTasks)
		log.Printf("[Mock] POST /api/transfer-tasks/failed-for-retry - returned %d tasks", len(failedTasks))
	})

	mux.HandleFunc("/api/transfer-tasks/reset", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")

		type Request struct {
			TaskID int64  `json:"task_id"`
			Status string `json:"status"`
		}
		var req Request
		json.NewDecoder(r.Body).Decode(&req)

		taskMutex.Lock()
		task, exists := taskStore[req.TaskID]
		if !exists {
			taskMutex.Unlock()
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{"error": "task not found"}`)
			log.Printf("[Mock] POST /api/transfer-tasks/reset - Task %d not found", req.TaskID)
			return
		}

		if task.Status != "FAILED" {
			taskMutex.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"error": "task is not FAILED, current status: %s"}`, task.Status)
			log.Printf("[Mock] POST /api/transfer-tasks/reset - Task %d is %s, cannot reset", req.TaskID, task.Status)
			return
		}

		task.Status = req.Status
		taskMutex.Unlock()

		log.Printf("[Mock] POST /api/transfer-tasks/reset - task_id: %d, status: %s", req.TaskID, req.Status)

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "reset"}`)
	})

	mux.HandleFunc("/api/transfer-tasks/list", func(w http.ResponseWriter, r *http.Request) {
		if simulateFailure() {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		simulateDelay()

		w.Header().Set("Content-Type", "application/json")

		taskMutex.RLock()
		var tasks []TransferTask
		for _, task := range taskStore {
			tasks = append(tasks, *task)
		}
		taskMutex.RUnlock()

		json.NewEncoder(w).Encode(tasks)
		log.Printf("[Mock] GET /api/transfer-tasks/list - returned %d tasks", len(tasks))
	})

	mux.HandleFunc("/api/transfer-tasks/delete", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		taskID := r.URL.Query().Get("task_id")
		var id int64
		fmt.Sscanf(taskID, "%d", &id)

		taskMutex.Lock()
		delete(taskStore, id)
		taskMutex.Unlock()

		log.Printf("[Mock] DELETE /api/transfer-tasks/delete - Task %d deleted", id)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "deleted"}`)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Mock server starting on :8080...")
		log.Println("Available presets: normal, high, critical, failure")
		log.Println("Example: POST /api/system/set-load?preset=critical")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down mock server...")
	server.Close()
	log.Println("Mock server stopped")
}
