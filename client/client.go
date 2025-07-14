package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type ClientConfig struct {
	ServerURL      string
	TotalRequests  int
	Concurrency    int
	RequestTimeout time.Duration
	TestDuration   time.Duration
	WarmupRequests int
	ReportInterval time.Duration
}

type Request struct {
	Number int `json:"number"`
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

type Metrics struct {
	TotalRequests      int64
	SuccessfulRequests int64
	FailedRequests     int64

	ErrorsByCode   map[string]int64
	ErrorsByStatus map[int]int64
	mu             sync.RWMutex

	TotalLatency int64
	MinLatency   int64
	MaxLatency   int64

	RateLimitHits    int64
	TimeoutErrors    int64
	ConnectionErrors int64

	StartTime       time.Time
	LastReportTime  time.Time
	LastReportCount int64
}

func NewMetrics() *Metrics {
	return &Metrics{
		ErrorsByCode:   make(map[string]int64),
		ErrorsByStatus: make(map[int]int64),
		MinLatency:     999999999,
		StartTime:      time.Now(),
		LastReportTime: time.Now(),
	}
}

type LoadTestClient struct {
	config     *ClientConfig
	httpClient *http.Client
	metrics    *Metrics
	logger     *slog.Logger
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewLoadTestClient(config *ClientConfig, logger *slog.Logger) (*LoadTestClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          config.Concurrency * 2,
		MaxIdleConnsPerHost:   config.Concurrency * 2,
		MaxConnsPerHost:       config.Concurrency * 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		DisableKeepAlives:     false,
		ResponseHeaderTimeout: config.RequestTimeout,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   config.RequestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Don't follow redirects
		},
	}

	return &LoadTestClient{
		config:     config,
		httpClient: httpClient,
		metrics:    NewMetrics(),
		logger:     logger,
		stopCh:     make(chan struct{}),
	}, nil
}

func (c *LoadTestClient) makeRequest(ctx context.Context, endpoint string, number int) error {
	startTime := time.Now()

	requestID := uuid.New().String()

	payload := Request{Number: number}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.config.ServerURL+endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("request creation error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", requestID)
	req.Header.Set("User-Agent", "LoadTestClient/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&c.metrics.ConnectionErrors, 1)

		if ctx.Err() != nil {
			atomic.AddInt64(&c.metrics.TimeoutErrors, 1)
		}

		return fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	latency := time.Since(startTime).Microseconds()

	atomic.AddInt64(&c.metrics.TotalRequests, 1)
	c.updateStatusMetrics(resp.StatusCode)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		atomic.AddInt64(&c.metrics.SuccessfulRequests, 1)
		atomic.AddInt64(&c.metrics.TotalLatency, latency)
		c.updateLatencyMetrics(latency)

		var successResp Response
		if err := json.Unmarshal(body, &successResp); err != nil {
			c.logger.Warn("Failed to parse success response",
				slog.String("request_id", requestID),
				slog.String("error", err.Error()))
		}

	case resp.StatusCode == http.StatusTooManyRequests:
		atomic.AddInt64(&c.metrics.RateLimitHits, 1)
		atomic.AddInt64(&c.metrics.FailedRequests, 1)
		c.updateErrorMetrics("RATE_LIMIT_EXCEEDED")

	case resp.StatusCode >= 400:
		atomic.AddInt64(&c.metrics.FailedRequests, 1)

		var errorResp ErrorResponse
		if err := json.Unmarshal(body, &errorResp); err == nil {
			c.updateErrorMetrics(errorResp.Code)

			if resp.StatusCode >= 500 {
				c.logger.Error("Server error",
					slog.String("request_id", requestID),
					slog.String("error_code", errorResp.Code),
					slog.String("error", errorResp.Error))
			}
		} else {
			c.updateErrorMetrics("UNKNOWN_ERROR")
		}

	default:
		atomic.AddInt64(&c.metrics.FailedRequests, 1)
		c.updateErrorMetrics("UNEXPECTED_STATUS")
	}

	return nil
}

func (c *LoadTestClient) updateLatencyMetrics(latency int64) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()

	if latency < c.metrics.MinLatency {
		c.metrics.MinLatency = latency
	}
	if latency > c.metrics.MaxLatency {
		c.metrics.MaxLatency = latency
	}
}

func (c *LoadTestClient) updateErrorMetrics(code string) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()
	c.metrics.ErrorsByCode[code]++
}

func (c *LoadTestClient) updateStatusMetrics(status int) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()
	c.metrics.ErrorsByStatus[status]++
}

func (c *LoadTestClient) worker(workCh <-chan int, endpoint string) {
	defer c.wg.Done()

	for {
		select {
		case number, ok := <-workCh:
			if !ok {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.config.RequestTimeout)

			if err := c.makeRequest(ctx, endpoint, number); err != nil {
				c.logger.Debug("Request failed",
					slog.String("error", err.Error()),
					slog.String("endpoint", endpoint))
			}

			cancel()

		case <-c.stopCh:
			return
		}
	}
}

func (c *LoadTestClient) RunLoadTest(endpoint string) error {
	c.logger.Info("Starting load test",
		slog.String("endpoint", endpoint),
		slog.Int("total_requests", c.config.TotalRequests),
		slog.Int("concurrency", c.config.Concurrency),
		slog.String("server", c.config.ServerURL))

	if c.config.WarmupRequests > 0 {
		c.logger.Info("Running warmup", slog.Int("requests", c.config.WarmupRequests))
		c.runWarmup(endpoint)
	}

	c.metrics = NewMetrics()

	workCh := make(chan int, c.config.Concurrency*2)

	for i := 0; i < c.config.Concurrency; i++ {
		c.wg.Add(1)
		go c.worker(workCh, endpoint)
	}

	c.startProgressReporter()

	c.wg.Add(1)
	go c.generateRequests(workCh)

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("Load test completed")
	case <-time.After(c.config.TestDuration):
		c.logger.Info("Load test timeout reached")
		close(c.stopCh)
	}

	// Cleanup
	close(workCh)
	c.wg.Wait()

	return nil
}

func (c *LoadTestClient) generateRequests(workCh chan<- int) {
	defer c.wg.Done()

	for i := 0; i < c.config.TotalRequests; i++ {
		select {
		case workCh <- i % 1000:
		case <-c.stopCh:
			return
		}
	}
}

func (c *LoadTestClient) runWarmup(endpoint string) {
	warmupConcurrency := min(c.config.Concurrency, 10)
	workCh := make(chan int, warmupConcurrency)

	var wg sync.WaitGroup
	for i := 0; i < warmupConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for number := range workCh {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = c.makeRequest(ctx, endpoint, number)
				cancel()
			}
		}()
	}

	for i := 0; i < c.config.WarmupRequests; i++ {
		workCh <- i % 100
	}
	close(workCh)

	wg.Wait()
}

func (c *LoadTestClient) startProgressReporter() {
	ticker := time.NewTicker(c.config.ReportInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.reportProgress()
			case <-c.stopCh:
				return
			}
		}
	}()
}

func (c *LoadTestClient) reportProgress() {
	current := atomic.LoadInt64(&c.metrics.TotalRequests)
	successful := atomic.LoadInt64(&c.metrics.SuccessfulRequests)
	failed := atomic.LoadInt64(&c.metrics.FailedRequests)

	now := time.Now()
	duration := now.Sub(c.metrics.StartTime).Seconds()
	intervalDuration := now.Sub(c.metrics.LastReportTime).Seconds()
	intervalRequests := current - c.metrics.LastReportCount

	overallRate := float64(current) / duration
	intervalRate := float64(intervalRequests) / intervalDuration

	c.metrics.LastReportTime = now
	c.metrics.LastReportCount = current

	c.logger.Info("Progress update",
		slog.Int64("total", current),
		slog.Int64("successful", successful),
		slog.Int64("failed", failed),
		slog.Float64("overall_rps", overallRate),
		slog.Float64("current_rps", intervalRate),
		slog.Int64("rate_limited", atomic.LoadInt64(&c.metrics.RateLimitHits)))
}

func (c *LoadTestClient) PrintFinalReport() {
	total := atomic.LoadInt64(&c.metrics.TotalRequests)
	successful := atomic.LoadInt64(&c.metrics.SuccessfulRequests)
	failed := atomic.LoadInt64(&c.metrics.FailedRequests)
	totalLatency := atomic.LoadInt64(&c.metrics.TotalLatency)
	rateLimited := atomic.LoadInt64(&c.metrics.RateLimitHits)
	timeouts := atomic.LoadInt64(&c.metrics.TimeoutErrors)
	connErrors := atomic.LoadInt64(&c.metrics.ConnectionErrors)

	duration := time.Since(c.metrics.StartTime)

	successRate := float64(0)
	avgLatency := float64(0)
	if total > 0 {
		successRate = float64(successful) / float64(total) * 100
	}
	if successful > 0 {
		avgLatency = float64(totalLatency) / float64(successful)
	}

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("            LOAD TEST RESULTS")
	fmt.Println(strings.Repeat("=", 50))

	fmt.Printf("\nTest Duration:        %v\n", duration.Round(time.Millisecond))
	fmt.Printf("Total Requests:       %d\n", total)
	fmt.Printf("Successful:           %d (%.2f%%)\n", successful, successRate)
	fmt.Printf("Failed:               %d (%.2f%%)\n", failed, float64(failed)/float64(total)*100)

	fmt.Println("\nPerformance Metrics:")
	fmt.Printf("  Requests/Second:    %.2f\n", float64(total)/duration.Seconds())
	fmt.Printf("  Avg Latency:        %.2f µs (%.2f ms)\n", avgLatency, avgLatency/1000)
	fmt.Printf("  Min Latency:        %d µs (%.2f ms)\n", c.metrics.MinLatency, float64(c.metrics.MinLatency)/1000)
	fmt.Printf("  Max Latency:        %d µs (%.2f ms)\n", c.metrics.MaxLatency, float64(c.metrics.MaxLatency)/1000)

	fmt.Println("\nError Breakdown:")
	fmt.Printf("  Rate Limited:       %d\n", rateLimited)
	fmt.Printf("  Timeouts:           %d\n", timeouts)
	fmt.Printf("  Connection Errors:  %d\n", connErrors)

	if len(c.metrics.ErrorsByStatus) > 0 {
		fmt.Println("\nStatus Code Distribution:")
		c.metrics.mu.RLock()
		for status, count := range c.metrics.ErrorsByStatus {
			percentage := float64(count) / float64(total) * 100
			fmt.Printf("  %d: %d (%.2f%%)\n", status, count, percentage)
		}
		c.metrics.mu.RUnlock()
	}

	if len(c.metrics.ErrorsByCode) > 0 {
		fmt.Println("\nError Code Distribution:")
		c.metrics.mu.RLock()
		for code, count := range c.metrics.ErrorsByCode {
			fmt.Printf("  %s: %d\n", code, count)
		}
		c.metrics.mu.RUnlock()
	}

	fmt.Println(strings.Repeat("=", 50))
}

func (c *LoadTestClient) GetServerMetrics() {
	resp, err := c.httpClient.Get(c.config.ServerURL + "/metrics")
	if err != nil {
		c.logger.Error("Failed to fetch server metrics", slog.String("error", err.Error()))
		return
	}
	defer resp.Body.Close()

	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		c.logger.Error("Failed to parse server metrics", slog.String("error", err.Error()))
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("            SERVER METRICS")
	fmt.Println(strings.Repeat("=", 50))

	for key, value := range metrics {
		fmt.Printf("%-20s: %v\n", key, value)
	}

	fmt.Println(strings.Repeat("=", 50))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Test scenarios
	scenarios := []struct {
		name        string
		endpoint    string
		requests    int
		concurrency int
		description string
	}{
		{
			name:        "Fast Endpoint - High Volume",
			endpoint:    "/api/v1/compute/fast",
			requests:    1_000_000,
			concurrency: 500,
			description: "Testing direct computation without worker pool",
		},
		{
			name:        "CPU Intensive - Moderate Volume",
			endpoint:    "/api/v1/compute/intensive",
			requests:    100_000,
			concurrency: 500,
			description: "Testing worker pool with CPU-intensive tasks",
		},
	}

	serverURL := "http://localhost:8080"

	fmt.Println("Checking server health...")
	resp, err := http.Get(serverURL + "/health")
	if err != nil {
		logger.Error("Server health check failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error("Server is not healthy", slog.Int("status", resp.StatusCode))
		os.Exit(1)
	}

	fmt.Println("Server is healthy. Starting load tests...\n")

	for _, scenario := range scenarios {
		fmt.Printf("\n%s\n", strings.Repeat("*", 50))
		fmt.Printf("Scenario: %s\n", scenario.name)
		fmt.Printf("Description: %s\n", scenario.description)
		fmt.Printf("%s\n\n", strings.Repeat("*", 50))

		config := &ClientConfig{
			ServerURL:      serverURL,
			TotalRequests:  scenario.requests,
			Concurrency:    scenario.concurrency,
			RequestTimeout: 30 * time.Second,
			TestDuration:   5 * time.Minute,
			WarmupRequests: 1000,
			ReportInterval: 5 * time.Second,
		}

		client, err := NewLoadTestClient(config, logger)
		if err != nil {
			logger.Error("Failed to create client", slog.String("error", err.Error()))
			continue
		}

		if err := client.RunLoadTest(scenario.endpoint); err != nil {
			logger.Error("Load test failed", slog.String("error", err.Error()))
			continue
		}

		client.PrintFinalReport()

		client.GetServerMetrics()

		if scenario.endpoint != scenarios[len(scenarios)-1].endpoint {
			fmt.Println("\nPausing for 10 seconds before next scenario...")
			time.Sleep(10 * time.Second)
		}
	}

	fmt.Println("\nAll load tests completed!")
}
