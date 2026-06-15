/*
 * Data Processing System - Java Implementation
 * MSCS-632-A01: Advanced Programming Languages
 * Murali Krishna Chintha
 *
 * Demonstrates: Concurrency with ReentrantLock, thread-safe shared queue,
 *               ExecutorService thread management, exception handling, logging.
 */

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;

public class DataProcessingSystem {

    // ---------------------------------------------------------------
    // Logger - outputs to console and to results.log
    // ---------------------------------------------------------------
    private static final Logger LOG = Logger.getLogger("DataProcessingSystem");

    static {
        try {
            LogManager.getLogManager().reset();
            ConsoleHandler ch = new ConsoleHandler();
            ch.setFormatter(new SimpleFormatter());
            ch.setLevel(Level.ALL);
            FileHandler fh = new FileHandler("results.log", false);
            fh.setFormatter(new SimpleFormatter());
            fh.setLevel(Level.ALL);
            LOG.addHandler(ch);
            LOG.addHandler(fh);
            LOG.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Logger setup failed: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // SharedQueue - thread-safe task queue using ReentrantLock
    // addTask() and getTask() are synchronized via lock
    // ---------------------------------------------------------------
    static class SharedQueue {
        private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        private final ReentrantLock lock = new ReentrantLock();

        public void addTask(String task) {
            lock.lock();
            try {
                queue.offer(task);
                LOG.info("[Queue] Task added: " + task);
            } finally {
                lock.unlock(); // always released in finally
            }
        }

        public String getTask() {
            lock.lock();
            try {
                String task = queue.poll();
                if (task != null) {
                    LOG.info("[Queue] Task retrieved: " + task);
                }
                return task;
            } finally {
                lock.unlock();
            }
        }

        public boolean isEmpty() { return queue.isEmpty(); }
        public int size()        { return queue.size(); }
    }

    // ---------------------------------------------------------------
    // SharedResults - thread-safe results list + file output
    // ---------------------------------------------------------------
    static class SharedResults {
        private final List<String> results = new ArrayList<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final String outputFile;

        SharedResults(String outputFile) {
            this.outputFile = outputFile;
        }

        public void addResult(String result) {
            lock.lock();
            try {
                results.add(result);
                writeToFile(result);
            } finally {
                lock.unlock();
            }
        }

        private void writeToFile(String result) {
            // try-catch handles IOException from file operations
            try (FileWriter fw = new FileWriter(outputFile, true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(result);
                bw.newLine();
            } catch (IOException e) {
                LOG.severe("[Results] File write error: " + e.getMessage());
            }
        }

        public List<String> getResults() {
            lock.lock();
            try {
                return new ArrayList<>(results);
            } finally {
                lock.unlock();
            }
        }
    }

    // ---------------------------------------------------------------
    // WorkerThread - retrieves tasks, processes them, saves results
    // ---------------------------------------------------------------
    static class WorkerThread implements Runnable {
        private final int workerID;
        private final SharedQueue queue;
        private final SharedResults results;

        WorkerThread(int id, SharedQueue queue, SharedResults results) {
            this.workerID = id;
            this.queue    = queue;
            this.results  = results;
        }

        @Override
        public void run() {
            LOG.info("[Worker-" + workerID + "] Started.");
            try {
                while (true) {
                    String task = queue.getTask();
                    if (task == null) break; // queue exhausted
                    String result = processTask(task);
                    results.addResult(result);
                }
                LOG.info("[Worker-" + workerID + "] Completed all tasks.");
            } catch (InterruptedException e) {
                // Handle thread interruption gracefully
                LOG.warning("[Worker-" + workerID + "] Interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.severe("[Worker-" + workerID + "] Unexpected error: " + e.getMessage());
            }
        }

        private String processTask(String task) throws InterruptedException {
            LOG.info("[Worker-" + workerID + "] Processing: " + task);
            Thread.sleep(150); // simulate computational work
            String result = "[Worker-" + workerID + "] DONE: " + task.toUpperCase();
            LOG.info("[Worker-" + workerID + "] Result ready: " + result);
            return result;
        }
    }

    // ---------------------------------------------------------------
    // MAIN
    // ---------------------------------------------------------------
    public static void main(String[] args) {
        System.out.println("==============================================");
        System.out.println("  DATA PROCESSING SYSTEM - Java              ");
        System.out.println("==============================================");

        // Initialize output file
        try (FileWriter fw = new FileWriter("results.txt", false)) {
            fw.write("Data Processing Results\n");
            fw.write("======================\n");
        } catch (IOException e) {
            LOG.severe("Could not initialize output file: " + e.getMessage());
        }

        SharedQueue    taskQueue     = new SharedQueue();
        SharedResults  sharedResults = new SharedResults("results.txt");

        // Populate queue with 10 tasks
        String[] tasks = {
            "task-001: analyze sales data",
            "task-002: generate monthly report",
            "task-003: process user records",
            "task-004: validate inventory",
            "task-005: compute risk scores",
            "task-006: sync external feeds",
            "task-007: archive old entries",
            "task-008: run audit checks",
            "task-009: export summary",
            "task-010: cleanup temp files"
        };
        for (String t : tasks) taskQueue.addTask(t);

        System.out.println("\n[Main] " + tasks.length + " tasks loaded into queue.");
        System.out.println("[Main] Launching 4 worker threads...\n");

        // ExecutorService manages the thread pool
        int numWorkers = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        for (int i = 1; i <= numWorkers; i++) {
            executor.submit(new WorkerThread(i, taskQueue, sharedResults));
        }

        // Graceful shutdown - wait up to 30s for all tasks
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warning("[Main] Timeout reached. Forcing shutdown.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.severe("[Main] Main thread interrupted: " + e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Print final summary
        System.out.println("\n==============================================");
        System.out.println("  PROCESSING COMPLETE");
        System.out.println("==============================================");
        List<String> finalResults = sharedResults.getResults();
        System.out.println("[Main] Total tasks completed: " + finalResults.size());
        System.out.println("[Main] Results also written to: results.txt and results.log");
        System.out.println("\n--- Final Results ---");
        for (String r : finalResults) System.out.println(r);
        System.out.println("==============================================");
    }
}
