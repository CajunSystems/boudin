package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A reference to one workflow execution, for collecting its result and inspecting its state
 * without holding the calling thread for the workflow's lifetime.
 *
 * <p>A handle needs nothing but a {@link SharedLog} and a workflow ID, so it works from a process
 * that neither started the workflow nor runs a worker:
 *
 * <pre>{@code
 * // In a request handler — returns as soon as the workflow is durably started
 * WorkflowHandle<OrderResult> handle = client.start(() -> stub.process(order));
 * return handle.workflowId();
 *
 * // In a later request, possibly another process
 * OrderResult result = client.getHandle(workflowId, OrderResult.class).getResult();
 * }</pre>
 *
 * <p>Handles are immutable and safe to share between threads.
 *
 * @param <T> the workflow method's return type. Unchecked: results are serialized with full class
 *            information, so the payload carries its own type and this parameter only shapes the
 *            call site, exactly as it does for a blocking stub.
 */
public final class WorkflowHandle<T> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowHandle.class);

    private final SharedLog sharedLog;
    private final String workflowId;

    WorkflowHandle(SharedLog sharedLog, String workflowId) {
        this.sharedLog = sharedLog;
        this.workflowId = workflowId;
    }

    /** The execution's workflow ID — the only thing needed to rebuild this handle elsewhere. */
    public String workflowId() {
        return workflowId;
    }

    /**
     * Blocks until the workflow completes and returns its result.
     *
     * <p>Returns immediately if the workflow has already finished, however long ago.
     *
     * @throws WorkflowFailureException if the workflow failed
     */
    public T getResult() {
        return join(getResultAsync(), null);
    }

    /**
     * Blocks for at most {@code timeout}.
     *
     * @throws WorkflowTimeoutException if the workflow has not finished in time
     * @throws WorkflowFailureException if the workflow failed
     */
    public T getResult(Duration timeout) {
        return join(getResultAsync(), timeout);
    }

    /**
     * Returns a future completed with the workflow's result, or completed exceptionally with
     * {@link WorkflowFailureException} if it failed.
     *
     * <p>The underlying log subscription is closed when the future completes, however it
     * completes, so a caller that abandons the future does not leak it.
     */
    public CompletableFuture<T> getResultAsync() {
        LogTag historyTag = LogTag.of("workflow-history", workflowId);
        LogView historyView = sharedLog.getView(historyTag);

        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();

        // Order matters. Take the tip, subscribe past it, then read up to it:
        //   - subscribing before reading means a terminal event landing between the two calls
        //     arrives on the subscription rather than being missed by both
        //   - reading from the tip rather than subscribing from BEGINNING means an already
        //     finished workflow resolves without re-delivering its whole history
        long tip = historyView.getLatestSeqnum();

        SharedLog.Subscription sub = historyView.subscribe(
                new LogPosition(tip + 1),
                entry -> completeFrom(entry, resultFuture));

        try {
            for (LogEntry entry : historyView.readAll().join()) {
                if (entry.seqnum() > tip) break;      // the subscription owns everything past tip
                completeFrom(entry, resultFuture);
                if (resultFuture.isDone()) break;
            }
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }

        return resultFuture
                .handle((bytes, error) -> {
                    try { sub.close(); } catch (Exception ignored) {}
                    if (error != null) throw wrap(error);
                    return this.<T>deserialize(bytes);
                });
    }

    /**
     * Reads the current state of the execution from its history.
     *
     * <p>Needs no running worker — unlike a {@code @QueryMethod}, which reports live in-memory
     * state and only while a worker hosts the workflow, this reports what the log durably says.
     *
     * @throws IllegalStateException if no history exists for this workflow ID
     */
    public WorkflowExecutionDescription describe() {
        List<HistoryEvent> history = readHistory();
        if (history.isEmpty()) {
            throw new IllegalStateException(
                    "No history for workflow '" + workflowId + "'. It was never started, "
                    + "or the workflow ID is wrong.");
        }

        HistoryEvent.WorkflowStarted started = history.stream()
                .filter(HistoryEvent.WorkflowStarted.class::isInstance)
                .map(HistoryEvent.WorkflowStarted.class::cast)
                .findFirst()
                .orElse(null);

        WorkflowStatus status = WorkflowStatus.RUNNING;
        java.time.Instant closedAt = null;
        WorkflowExecutionDescription.Failure failure = null;

        for (HistoryEvent event : history) {
            if (event instanceof HistoryEvent.WorkflowCompleted completed) {
                status = WorkflowStatus.COMPLETED;
                closedAt = completed.timestamp();
            } else if (event instanceof HistoryEvent.WorkflowFailed failed) {
                status = WorkflowStatus.FAILED;
                closedAt = failed.timestamp();
                failure = new WorkflowExecutionDescription.Failure(
                        failed.errorType(), failed.message());
            }
        }

        return new WorkflowExecutionDescription(
                workflowId,
                started != null ? started.workflowType() : null,
                started != null ? started.taskQueue() : null,
                status,
                started != null ? started.timestamp() : null,
                closedAt,
                history.size(),
                failure);
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void completeFrom(LogEntry entry, CompletableFuture<byte[]> future) {
        HistoryEvent event;
        try {
            event = HistorySerializer.INSTANCE.deserialize(entry.data());
        } catch (Exception e) {
            // An event this build cannot read must not hide a terminal event further along.
            log.warn("Skipping undeserializable history entry at seqnum {} for workflow {}: {}",
                    entry.seqnum(), workflowId, e.toString());
            return;
        }
        switch (event) {
            case HistoryEvent.WorkflowCompleted completed -> future.complete(completed.result());
            case HistoryEvent.WorkflowFailed failed -> future.completeExceptionally(
                    new WorkflowFailureException(
                            failed.workflowId(), failed.errorType(), failed.message()));
            default -> { }
        }
    }

    private List<HistoryEvent> readHistory() {
        return sharedLog.getView(LogTag.of("workflow-history", workflowId))
                .readAll().join().stream()
                .map(entry -> {
                    try {
                        return HistorySerializer.INSTANCE.deserialize(entry.data());
                    } catch (Exception e) {
                        log.warn("Skipping undeserializable history entry at seqnum {} for {}: {}",
                                entry.seqnum(), workflowId, e.toString());
                        return null;
                    }
                })
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    @SuppressWarnings("unchecked")
    private <R> R deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        return (R) KryoSerializer.fromBytes(bytes);
    }

    private T join(CompletableFuture<T> future, Duration timeout) {
        try {
            return timeout == null
                    ? future.get()
                    : future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new WorkflowTimeoutException(workflowId, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for workflow " + workflowId, e);
        } catch (ExecutionException e) {
            throw wrap(e.getCause());
        }
    }

    /** Preserves {@link WorkflowFailureException} rather than burying it in a wrapper. */
    private static RuntimeException wrap(Throwable t) {
        if (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) {
            return wrap(t.getCause());
        }
        if (t instanceof RuntimeException re) return re;
        return new IllegalStateException(t);
    }
}
