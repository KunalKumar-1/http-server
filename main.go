package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Configuration struct {
	Port            string
	WorkerCount     int
	QueueSize       int
	ShutdownTimeout time.Duration
	RequestTimeout  time.Duration
	RateLimit       int
	Environment     string
}

type Metrics struct {
	TotalRequests  int64
	SuccessCount   int64
	ErrorCount     int64
	ActiveRequests int64
	TotalLatency   int64 // microseconds
	QueueDepth     int64
	RejectedCount  int64
}

type Server struct {
	config      *Configuration
	logger      *slog.Logger
	metrics     *Metrics
	workerPool  *WorkerPool
	rateLimiter *RateLimiter
	healthCheck *HealthChecker
}

type Request struct {
	Number int `json:"number" binding:"required,min=0,max=1000000"`
}

type Response struct {
	RequestID string      `json:"request_id"`
	JobID     int64       `json:"job_id"`
	Result    interface{} `json:"result"`
	Latency   int64       `json:"latency_us"`
	Timestamp int64       `json:"timestamp"`
}

type ErrorResponse struct {
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
	Code      string `json:"code"`
	Timestamp int64  `json:"timestamp"`
}

type WorkerPool struct {
	workers    int
	jobQueue   chan Job
	results    chan Result
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	logger     *slog.Logger
	metrics    *Metrics
	jobCounter int64
}

type Job struct {
	ID        int64
	RequestID string
	Data      interface{}
	Type      string
	Context   context.Context
}

type Result struct {
	JobID     int64
	RequestID string
	Data      interface{}
	Error     error
	Duration  time.Duration
}

func NewServer(config *Configuration, logger *slog.Logger) (*Server, error) {
	if config == nil {
		return nil, errors.New("configuration is required")
	}

	metrics := &Metrics{}

	wp := NewWorkerPool(config.WorkerCount, config.QueueSize, logger, metrics)

	rateLimiter := NewRateLimiter(config.RateLimit)

	healthCheck := NewHealthChecker()

	return &Server{
		config:      config,
		logger:      logger,
		metrics:     metrics,
		workerPool:  wp,
		rateLimiter: rateLimiter,
		healthCheck: healthCheck,
	}, nil
}

func NewWorkerPool(workers, queueSize int, logger *slog.Logger, metrics *Metrics) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workers:  workers,
		jobQueue: make(chan Job, queueSize),
		results:  make(chan Result, queueSize),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logger,
		metrics:  metrics,
	}
}

func (wp *WorkerPool) Start() error {
	if wp.workers <= 0 {
		return errors.New("invalid worker count")
	}

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	wp.logger.Info("Worker pool started",
		slog.Int("workers", wp.workers),
		slog.Int("queue_size", cap(wp.jobQueue)))

	return nil
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	wp.logger.Debug("Worker started", slog.Int("worker_id", id))

	for {
		select {
		case job := <-wp.jobQueue:
			// Update queue depth metric
			atomic.StoreInt64(&wp.metrics.QueueDepth, int64(len(wp.jobQueue)))

			func() {
				defer func() {
					if r := recover(); r != nil {
						wp.logger.Error("Worker panic recovered",
							slog.Int("worker_id", id),
							slog.Any("panic", r),
							slog.String("request_id", job.RequestID))

						wp.results <- Result{
							JobID:     job.ID,
							RequestID: job.RequestID,
							Error:     fmt.Errorf("internal error: %v", r),
						}
					}
				}()

				startTime := time.Now()
				result := wp.processJob(job)
				result.Duration = time.Since(startTime)

				select {
				case wp.results <- result:
				case <-job.Context.Done():
					wp.logger.Warn("Job context cancelled",
						slog.Int64("job_id", job.ID),
						slog.String("request_id", job.RequestID))
				}
			}()

		case <-wp.ctx.Done():
			wp.logger.Debug("Worker stopping", slog.Int("worker_id", id))
			return
		}
	}
}

func (wp *WorkerPool) processJob(job Job) Result {
	result := Result{
		JobID:     job.ID,
		RequestID: job.RequestID,
	}

	switch job.Type {
	case "cpu_intensive":
		if num, ok := job.Data.(int); ok {
			computed := num
			for i := 0; i < 10000; i++ {
				computed = (computed * 31) % 1000000
			}
			result.Data = computed
		} else {
			result.Error = errors.New("invalid data type for cpu_intensive job")
		}

	case "complex":
		time.Sleep(5 * time.Millisecond)
		if num, ok := job.Data.(int); ok {
			result.Data = num * num
		} else {
			result.Error = errors.New("invalid data type for complex job")
		}

	default:
		result.Error = fmt.Errorf("unknown job type: %s", job.Type)
	}

	return result
}

func (wp *WorkerPool) Submit(ctx context.Context, jobType string, data interface{}, requestID string) (Result, error) {
	jobID := atomic.AddInt64(&wp.jobCounter, 1)

	// prepare a job
	job := Job{
		ID:        jobID,
		RequestID: requestID,
		Data:      data,
		Type:      jobType,
		Context:   ctx,
	}

	// try
	select {
	case wp.jobQueue <- job:
		// queue depth
		atomic.StoreInt64(&wp.metrics.QueueDepth, int64(len(wp.jobQueue)))

		// wait
		select {
		case result := <-wp.results:
			return result, nil
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}

	case <-ctx.Done():
		return Result{}, ctx.Err()

	default:
		// job queue is full
		atomic.AddInt64(&wp.metrics.RejectedCount, 1)
		return Result{}, errors.New("job queue full")
	}
}

func (wp *WorkerPool) Stop() {
	wp.logger.Info("Stopping worker pool")
	wp.cancel()
	wp.wg.Wait()
	close(wp.jobQueue)
	close(wp.results)
}

// token bucket rate limiting
type RateLimiter struct {
	rate   int
	bucket chan struct{}
	ticker *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// new rate limiter
func NewRateLimiter(rate int) *RateLimiter {
	if rate <= 0 {
		rate = 10000
	}

	rl := &RateLimiter{
		rate:   rate,
		bucket: make(chan struct{}, rate),
		ticker: time.NewTicker(time.Second / time.Duration(rate)),
		stopCh: make(chan struct{}),
	}

	for i := 0; i < rate; i++ {
		rl.bucket <- struct{}{}
	}

	rl.wg.Add(1)
	go rl.refill()

	return rl
}

// adds token to bucket
func (rl *RateLimiter) refill() {
	defer rl.wg.Done()

	for {
		select {
		case <-rl.ticker.C:
			select {
			case rl.bucket <- struct{}{}:
			default:
				// full
			}
		case <-rl.stopCh:
			return
		}
	}
}

// allow checks
func (rl *RateLimiter) Allow() bool {
	select {
	case <-rl.bucket:
		return true
	default:
		return false
	}
}

// stops the rate limiter
func (rl *RateLimiter) Stop() {
	close(rl.stopCh)
	rl.ticker.Stop()
	rl.wg.Wait()
}

// manages health checkes
type HealthChecker struct {
	checks map[string]func() error
	mu     sync.RWMutex
}

func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		checks: make(map[string]func() error),
	}
}

func (hc *HealthChecker) RegisterCheck(name string, check func() error) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.checks[name] = check
}

// run all
func (hc *HealthChecker) Check() map[string]string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	results := make(map[string]string)
	for name, check := range hc.checks {
		if err := check(); err != nil {
			results[name] = "unhealthy: " + err.Error()
		} else {
			results[name] = "healthy"
		}
	}

	return results
}

// middlewares

// id middleware
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// log middleware
func LoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()

		c.Next()

		logger.Info("Request processed",
			slog.String("request_id", c.GetString("request_id")),
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(startTime)),
			slog.String("client_ip", c.ClientIP()),
			slog.String("user_agent", c.Request.UserAgent()),
		)
	}
}

// update request matrics
func MetricsMiddleware(metrics *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		atomic.AddInt64(&metrics.TotalRequests, 1)
		atomic.AddInt64(&metrics.ActiveRequests, 1)

		startTime := time.Now()
		c.Next()

		atomic.AddInt64(&metrics.ActiveRequests, -1)
		atomic.AddInt64(&metrics.TotalLatency, time.Since(startTime).Microseconds())

		if c.Writer.Status() >= 200 && c.Writer.Status() < 400 {
			atomic.AddInt64(&metrics.SuccessCount, 1)
		} else {
			atomic.AddInt64(&metrics.ErrorCount, 1)
		}
	}
}

// rate limiting middleware
func RateLimitMiddleware(rl *RateLimiter, metrics *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !rl.Allow() {
			atomic.AddInt64(&metrics.RejectedCount, 1)
			c.JSON(http.StatusTooManyRequests, ErrorResponse{
				RequestID: c.GetString("request_id"),
				Error:     "Rate limit exceeded",
				Code:      "RATE_LIMIT_EXCEEDED",
				Timestamp: time.Now().Unix(),
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// timeout middleware
func TimeoutMiddleware(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)

		finished := make(chan struct{})
		panicChan := make(chan interface{})

		go func() {
			defer func() {
				if p := recover(); p != nil {
					panicChan <- p
				}
			}()
			c.Next()
			finished <- struct{}{}
		}()

		select {
		case <-finished:
		case p := <-panicChan:
			panic(p)
		case <-ctx.Done():
			c.JSON(http.StatusRequestTimeout, ErrorResponse{
				RequestID: c.GetString("request_id"),
				Error:     "Request timeout",
				Code:      "REQUEST_TIMEOUT",
				Timestamp: time.Now().Unix(),
			})
			c.Abort()
		}
	}
}

// Handler functions

func (s *Server) HandleComputeFast(c *gin.Context) {
	requestID := c.GetString("request_id")
	startTime := time.Now()

	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		s.logger.Error("Invalid request",
			slog.String("request_id", requestID),
			slog.String("error", err.Error()))

		c.JSON(http.StatusBadRequest, ErrorResponse{
			RequestID: requestID,
			Error:     "Invalid request format",
			Code:      "INVALID_REQUEST",
			Timestamp: time.Now().Unix(),
		})
		return
	}

	result := req.Number * req.Number

	c.JSON(http.StatusOK, Response{
		RequestID: requestID,
		JobID:     atomic.AddInt64(&s.workerPool.jobCounter, 1),
		Result:    result,
		Latency:   time.Since(startTime).Microseconds(),
		Timestamp: time.Now().Unix(),
	})
}

func (s *Server) HandleComputeIntensive(c *gin.Context) {
	requestID := c.GetString("request_id")
	startTime := time.Now()

	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		s.logger.Error("Invalid request",
			slog.String("request_id", requestID),
			slog.String("error", err.Error()))

		c.JSON(http.StatusBadRequest, ErrorResponse{
			RequestID: requestID,
			Error:     "Invalid request format",
			Code:      "INVALID_REQUEST",
			Timestamp: time.Now().Unix(),
		})
		return
	}

	ctx := c.Request.Context()
	result, err := s.workerPool.Submit(ctx, "cpu_intensive", req.Number, requestID)

	if err != nil {
		s.logger.Error("Worker pool error",
			slog.String("request_id", requestID),
			slog.String("error", err.Error()))

		statusCode := http.StatusInternalServerError
		errorCode := "INTERNAL_ERROR"

		if errors.Is(err, context.DeadlineExceeded) {
			statusCode = http.StatusRequestTimeout
			errorCode = "TIMEOUT"
		} else if err.Error() == "job queue full" {
			statusCode = http.StatusServiceUnavailable
			errorCode = "SERVICE_OVERLOADED"
		}

		c.JSON(statusCode, ErrorResponse{
			RequestID: requestID,
			Error:     err.Error(),
			Code:      errorCode,
			Timestamp: time.Now().Unix(),
		})
		return
	}

	if result.Error != nil {
		s.logger.Error("Processing error",
			slog.String("request_id", requestID),
			slog.String("error", result.Error.Error()))

		c.JSON(http.StatusInternalServerError, ErrorResponse{
			RequestID: requestID,
			Error:     result.Error.Error(),
			Code:      "PROCESSING_ERROR",
			Timestamp: time.Now().Unix(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		RequestID: requestID,
		JobID:     result.JobID,
		Result:    result.Data,
		Latency:   time.Since(startTime).Microseconds(),
		Timestamp: time.Now().Unix(),
	})
}

func (s *Server) HandleHealth(c *gin.Context) {
	health := s.healthCheck.Check()

	allHealthy := true
	for _, status := range health {
		if status != "healthy" {
			allHealthy = false
			break
		}
	}

	statusCode := http.StatusOK
	if !allHealthy {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, gin.H{
		"status":     health,
		"timestamp":  time.Now().Unix(),
		"goroutines": runtime.NumGoroutine(),
		"version":    "1.0.0",
	})
}

func (s *Server) HandleMetrics(c *gin.Context) {
	total := atomic.LoadInt64(&s.metrics.TotalRequests)
	success := atomic.LoadInt64(&s.metrics.SuccessCount)
	errors := atomic.LoadInt64(&s.metrics.ErrorCount)
	active := atomic.LoadInt64(&s.metrics.ActiveRequests)
	rejected := atomic.LoadInt64(&s.metrics.RejectedCount)
	totalLatency := atomic.LoadInt64(&s.metrics.TotalLatency)
	queueDepth := atomic.LoadInt64(&s.metrics.QueueDepth)

	avgLatency := float64(0)
	if success > 0 {
		avgLatency = float64(totalLatency) / float64(success)
	}

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	c.JSON(http.StatusOK, gin.H{
		"total_requests":  total,
		"success_count":   success,
		"error_count":     errors,
		"active_requests": active,
		"rejected_count":  rejected,
		"success_rate":    successRate,
		"avg_latency_us":  avgLatency,
		"queue_depth":     queueDepth,
		"worker_count":    s.workerPool.workers,
		"goroutines":      runtime.NumGoroutine(),
		"timestamp":       time.Now().Unix(),
	})
}

func SetupRouter(server *Server) *gin.Engine {
	if server.config.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(RequestIDMiddleware())
	router.Use(LoggerMiddleware(server.logger))
	router.Use(MetricsMiddleware(server.metrics))

	router.GET("/health", server.HandleHealth)
	router.GET("/metrics", server.HandleMetrics)

	api := router.Group("/api/v1")
	api.Use(RateLimitMiddleware(server.rateLimiter, server.metrics))
	api.Use(TimeoutMiddleware(server.config.RequestTimeout))
	{
		api.POST("/compute/fast", server.HandleComputeFast)
		api.POST("/compute/intensive", server.HandleComputeIntensive)

		api.POST("/compute", server.HandleComputeFast)
		api.GET("/stats", server.HandleMetrics)
	}

	return router
}

func LoadConfiguration() *Configuration {
	return &Configuration{
		Port:            getEnv("SERVER_PORT", ":8080"),
		WorkerCount:     getEnvInt("WORKER_COUNT", runtime.NumCPU()*2),
		QueueSize:       getEnvInt("QUEUE_SIZE", 10000),
		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		RequestTimeout:  getEnvDuration("REQUEST_TIMEOUT", 30*time.Second),
		RateLimit:       100000,
		Environment:     getEnv("ENVIRONMENT", "development"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func main() {
	config := LoadConfiguration()

	logLevel := slog.LevelInfo
	if config.Environment == "development" {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	server, err := NewServer(config, logger)
	if err != nil {
		logger.Error("Failed to create server", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := server.workerPool.Start(); err != nil {
		logger.Error("Failed to start worker pool", slog.String("error", err.Error()))
		os.Exit(1)
	}

	server.healthCheck.RegisterCheck("worker_pool", func() error {
		queueDepth := atomic.LoadInt64(&server.metrics.QueueDepth)
		if queueDepth > int64(config.QueueSize)*90/100 {
			return fmt.Errorf("queue depth too high: %d", queueDepth)
		}
		return nil
	})

	router := SetupRouter(server)

	srv := &http.Server{
		Addr:           config.Port,
		Handler:        router,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   15 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		logger.Info("Server starting",
			slog.String("port", config.Port),
			slog.Int("workers", config.WorkerCount),
			slog.Int("queue_size", config.QueueSize),
			slog.String("environment", config.Environment))

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown error", slog.String("error", err.Error()))
	}

	server.workerPool.Stop()

	server.rateLimiter.Stop()

	logger.Info("Server stopped",
		slog.Int64("total_requests", atomic.LoadInt64(&server.metrics.TotalRequests)),
		slog.Int64("success_count", atomic.LoadInt64(&server.metrics.SuccessCount)),
		slog.Int64("error_count", atomic.LoadInt64(&server.metrics.ErrorCount)),
		slog.Int64("rejected_count", atomic.LoadInt64(&server.metrics.RejectedCount)))
}
