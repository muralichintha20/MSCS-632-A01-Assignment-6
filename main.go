/*
 * Data Processing System - Go Implementation
 * MSCS-632-A01: Advanced Programming Languages
 * Murali Krishna Chintha
 *
 * Demonstrates: Goroutines, channels as concurrency-safe queue,
 *               sync.Mutex for shared results, defer for cleanup,
 *               idiomatic Go error handling.
 */

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------
// setupLogger - logs to console and to results.log
// ---------------------------------------------------------------
func setupLogger() (*log.Logger, *os.File) {
	logFile, err := os.OpenFile("results.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("[Logger] Failed to open log file: %v", err)
	}
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	log.SetOutput(logFile)
	return logger, logFile
}

// ---------------------------------------------------------------
// SharedResults - thread-safe results list + file writer
// Uses sync.Mutex to protect concurrent writes
// ---------------------------------------------------------------
type SharedResults struct {
	mu      sync.Mutex
	results []string
	file    *os.File
	writer  *bufio.Writer
}

func NewSharedResults(filename string) (*SharedResults, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open results file: %w", err)
	}
	w := bufio.NewWriter(f)
	w.WriteString("Data Processing Results\n")
	w.WriteString("======================\n")
	w.Flush()
	return &SharedResults{file: f, writer: w}, nil
}

func (sr *SharedResults) AddResult(result string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock() // defer ensures mutex is always released

	sr.results = append(sr.results, result)

	// Write to file - check error idiomatically
	_, err := sr.writer.WriteString(result + "\n")
	if err != nil {
		return fmt.Errorf("file write error: %w", err)
	}
	return sr.writer.Flush()
}

func (sr *SharedResults) GetResults() []string {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	// Return a copy to avoid external mutation
	cp := make([]string, len(sr.results))
	copy(cp, sr.results)
	return cp
}

func (sr *SharedResults) Close() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.writer.Flush()
	sr.file.Close()
}

// ---------------------------------------------------------------
// processTask - simulates computational work with a delay
// Returns result string and an error (idiomatic Go error handling)
// ---------------------------------------------------------------
func processTask(workerID int, task string, logger *log.Logger) (string, error) {
	logger.Printf("[Worker-%d] Processing: %s", workerID, task)

	// Simulate work
	time.Sleep(150 * time.Millisecond)

	if task == "" {
		return "", fmt.Errorf("worker-%d: received empty task", workerID)
	}

	result := fmt.Sprintf("[Worker-%d] DONE: %s", workerID, strings.ToUpper(task))
	logger.Printf("[Worker-%d] Result ready: %s", workerID, result)
	return result, nil
}

// ---------------------------------------------------------------
// worker - goroutine that reads from taskCh channel and writes results
// wg.Done() signals completion to the WaitGroup
// ---------------------------------------------------------------
func worker(id int, taskCh <-chan string, results *SharedResults, wg *sync.WaitGroup, logger *log.Logger) {
	defer wg.Done() // always called when goroutine exits

	logger.Printf("[Worker-%d] Started.", id)

	for task := range taskCh {
		// Channel range exits automatically when channel is closed
		result, err := processTask(id, task, logger)
		if err != nil {
			logger.Printf("[Worker-%d] Error processing task: %v", id, err)
			continue // skip failed task, continue with next
		}

		if err := results.AddResult(result); err != nil {
			logger.Printf("[Worker-%d] Error saving result: %v", id, err)
		}
	}

	logger.Printf("[Worker-%d] Completed all tasks.", id)
}

// ---------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------
func main() {
	fmt.Println("==============================================")
	fmt.Println("  DATA PROCESSING SYSTEM - Go               ")
	fmt.Println("==============================================")

	// Setup logger
	logger, logFile := setupLogger()
	defer logFile.Close() // defer ensures log file is closed on exit

	// Setup shared results file
	sharedResults, err := NewSharedResults("results.txt")
	if err != nil {
		logger.Fatalf("[Main] Failed to create results file: %v", err)
	}
	defer sharedResults.Close()

	// Define tasks
	tasks := []string{
		"task-001: analyze sales data",
		"task-002: generate monthly report",
		"task-003: process user records",
		"task-004: validate inventory",
		"task-005: compute risk scores",
		"task-006: sync external feeds",
		"task-007: archive old entries",
		"task-008: run audit checks",
		"task-009: export summary",
		"task-010: cleanup temp files",
	}

	// Channel acts as the concurrency-safe shared queue
	// Buffered to hold all tasks before workers start
	taskCh := make(chan string, len(tasks))

	// Load tasks into the channel
	for _, t := range tasks {
		taskCh <- t
		logger.Printf("[Queue] Task added: %s", t)
	}
	close(taskCh) // closing signals workers that no more tasks will come

	fmt.Printf("\n[Main] %d tasks loaded into channel queue.\n", len(tasks))
	fmt.Println("[Main] Launching 4 goroutine workers...\n")

	// WaitGroup tracks goroutine completion
	var wg sync.WaitGroup
	numWorkers := 4

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, taskCh, sharedResults, &wg, logger)
	}

	// Block until all goroutines call wg.Done()
	wg.Wait()

	// Print summary
	fmt.Println("\n==============================================")
	fmt.Println("  PROCESSING COMPLETE")
	fmt.Println("==============================================")
	finalResults := sharedResults.GetResults()
	fmt.Printf("[Main] Total tasks completed: %d\n", len(finalResults))
	fmt.Println("[Main] Results also written to: results.txt and results.log")
	fmt.Println("\n--- Final Results ---")
	for _, r := range finalResults {
		fmt.Println(r)
	}
	fmt.Println("==============================================")
}
