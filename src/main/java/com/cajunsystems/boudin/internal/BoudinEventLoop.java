package com.cajunsystems.boudin.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Single-threaded event loop for Boudin worker infrastructure.
 *
 * Drives event delivery and timer scheduling on one named platform thread per worker.
 * User workflow and activity code runs on virtual threads launched from submitted tasks.
 */
public class BoudinEventLoop implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BoudinEventLoop.class);

    private final ScheduledExecutorService executor;

    public BoudinEventLoop(String taskQueue) {
        this.executor = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform()
                        .name("boudin-event-loop-" + taskQueue)
                        .daemon(true)
                        .unstarted(r));
    }

    /** Submit a task to execute on the event loop thread. */
    public void submit(Runnable task) {
        executor.execute(() -> {
            try {
                task.run();
            } catch (Exception e) {
                log.error("Uncaught exception in event loop task", e);
            }
        });
    }

    /**
     * Schedule a task to run after {@code delayMs} milliseconds on the event loop thread.
     *
     * @return a handle that can be used to cancel the timer before it fires
     */
    public ScheduledFuture<?> schedule(Runnable task, long delayMs) {
        return executor.schedule(() -> {
            try {
                task.run();
            } catch (Exception e) {
                log.error("Uncaught exception in scheduled event loop task", e);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Shuts down the event loop, waiting up to 5 seconds for queued tasks to drain.
     * Call this AFTER all subscriptions are closed so no new tasks are submitted.
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("BoudinEventLoop: did not drain within 5 seconds; forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
