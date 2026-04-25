package com.cajunsystems.boudin.workflow;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.internal.BoudinMetrics;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Hosts a single workflow instance on a Java 21 virtual thread.
 *
 * <p>Responsible for:
 * <ol>
 *   <li>Starting the workflow virtual thread with the correct {@link WorkflowContext}
 *       installed via {@link #CURRENT_CONTEXT}.</li>
 *   <li>Invoking the workflow method via reflection with deserialized arguments.</li>
 *   <li>Appending {@code WorkflowCompleted} or {@code WorkflowFailed} to the history
 *       log when the workflow method returns or throws.</li>
 *   <li>Completing {@link WorkflowContext#completionFuture} for client stubs that are
 *       blocking on the result.</li>
 * </ol>
 *
 * <p>The workflow virtual thread is created with {@link Thread#ofVirtual()} — it
 * blocks cheaply via {@link java.util.concurrent.CompletableFuture#join()} and
 * {@code Object.wait()} without consuming a platform thread.
 */
public class WorkflowThread {

    private static final Logger log = LoggerFactory.getLogger(WorkflowThread.class);

    /**
     * ThreadLocal that holds the current {@link WorkflowContext}.
     * Set before the workflow method runs; cleared when it finishes.
     * Accessed by {@link Workflow} static methods.
     */
    static final ThreadLocal<WorkflowContext> CURRENT_CONTEXT = new ThreadLocal<>();

    private final WorkflowContext context;
    private final Object workflowImpl;
    private final Method workflowMethod;
    private final byte[] inputBytes;
    private final BoudinMetrics metrics;
    private final Thread thread;

    public WorkflowThread(WorkflowContext context, Object workflowImpl,
                          Method workflowMethod, byte[] inputBytes, BoudinMetrics metrics) {
        this.context = context;
        this.workflowImpl = workflowImpl;
        this.workflowMethod = workflowMethod;
        this.inputBytes = inputBytes;
        this.metrics = metrics;
        this.thread = Thread.ofVirtual()
                .name("boudin-workflow-" + context.workflowId)
                .unstarted(this::run);
    }

    /** Starts the virtual thread running the workflow method. */
    public void start() {
        context.workflowThread = thread;
        thread.start();
    }

    /** Returns the underlying virtual thread (for interruption on shutdown). */
    public Thread thread() {
        return thread;
    }

    /** Returns the workflow implementation instance (needed for signal delivery). */
    public Object impl() {
        return workflowImpl;
    }

    public String workflowType() {
        return context.workflowType;
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void run() {
        CURRENT_CONTEXT.set(context);
        long startNs = System.nanoTime();
        metrics.workflowStarted(context.workflowType);
        try {
            // Deserialize args and invoke the @WorkflowMethod
            Object[] args = KryoSerializer.fromBytes(inputBytes);
            if (args == null) args = new Object[0];

            Object result = workflowMethod.invoke(workflowImpl, args);

            byte[] resultBytes = (workflowMethod.getReturnType() == Void.TYPE)
                    ? null
                    : KryoSerializer.toBytes(result);

            appendCompleted(resultBytes);
            metrics.workflowCompleted(context.workflowType, System.nanoTime() - startNs);
            context.completionFuture.complete(resultBytes);

        } catch (java.lang.reflect.InvocationTargetException ite) {
            Throwable cause = ite.getCause() != null ? ite.getCause() : ite;
            appendFailed(cause);
            metrics.workflowFailed(context.workflowType, System.nanoTime() - startNs);
            context.completionFuture.completeExceptionally(cause);
        } catch (Exception e) {
            appendFailed(e);
            metrics.workflowFailed(context.workflowType, System.nanoTime() - startNs);
            context.completionFuture.completeExceptionally(e);
        } finally {
            CURRENT_CONTEXT.remove();
        }
    }

    private void appendCompleted(byte[] resultBytes) {
        try {
            LogTag historyTag = LogTag.of("workflow-history", context.workflowId);
            HistoryEvent.WorkflowCompleted event = new HistoryEvent.WorkflowCompleted(
                    UUID.randomUUID().toString(),
                    Instant.now(),
                    context.workflowId,
                    resultBytes
            );
            context.sharedLog.append(
                    AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(event))
            ).join();
            log.debug("Workflow {} completed", context.workflowId);
        } catch (Exception e) {
            log.error("Failed to append WorkflowCompleted for {}", context.workflowId, e);
        }
    }

    private void appendFailed(Throwable cause) {
        try {
            LogTag historyTag = LogTag.of("workflow-history", context.workflowId);
            HistoryEvent.WorkflowFailed event = new HistoryEvent.WorkflowFailed(
                    UUID.randomUUID().toString(),
                    Instant.now(),
                    context.workflowId,
                    cause.getClass().getName(),
                    cause.getMessage()
            );
            context.sharedLog.append(
                    AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(event))
            ).join();
            log.warn("Workflow {} failed: {}", context.workflowId, cause.getMessage());
        } catch (Exception e) {
            log.error("Failed to append WorkflowFailed for {}", context.workflowId, e);
        }
    }
}
