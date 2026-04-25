package com.cajunsystems.boudin.workflow;

import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * InvocationHandler that dispatches child workflow invocations through the shared log.
 *
 * <p>On each {@code @WorkflowMethod} call:
 * <ol>
 *   <li>Generates a deterministic child workflow ID from parent ID + type + sequence.</li>
 *   <li>During replay, returns cached result or throws cached failure immediately.</li>
 *   <li>Live: appends {@code ChildWorkflowStarted} to parent history, atomically writes
 *       {@code WorkflowStarted} to child task-queue and child history, starts a watcher
 *       virtual thread, then parks the parent workflow thread.</li>
 * </ol>
 *
 * <p>Crash recovery: if {@code ChildWorkflowStarted} is already in parent history but the child
 * is not done, re-writes {@code WorkflowStarted} to child task-queue and child history.
 * {@code WorkflowDispatcher} deduplicates via {@code knownWorkflowIds}.
 */
class ChildWorkflowStub implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(ChildWorkflowStub.class);

    private final Class<?> childInterface;
    private final ChildWorkflowOptions options;

    ChildWorkflowStub(Class<?> childInterface, ChildWorkflowOptions options) {
        this.childInterface = childInterface;
        this.options = options;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!method.isAnnotationPresent(WorkflowMethod.class)) {
            return switch (method.getName()) {
                case "toString" -> "ChildWorkflowStub[" + childInterface.getSimpleName() + "]";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals"   -> proxy == args[0];
                default -> throw new UnsupportedOperationException(
                        "ChildWorkflowStub does not support method: " + method.getName());
            };
        }

        WorkflowContext ctx = Workflow.currentContext();
        String childWorkflowType = childInterface.getSimpleName();

        // Deterministic child ID: either user-specified or auto-generated per-type sequence
        int seq = ctx.replayState.nextChildWorkflowSequence(childWorkflowType);
        String childWorkflowId = (options.workflowId() != null)
                ? options.workflowId()
                : ctx.workflowId + ":child:" + childWorkflowType + ":" + seq;

        // Replay cache: completed
        if (ctx.replayState.hasChildWorkflowCompleted(childWorkflowId)) {
            byte[] result = ctx.replayState.getChildWorkflowResult(childWorkflowId);
            if (method.getReturnType() == Void.TYPE || result == null || result.length == 0) return null;
            return KryoSerializer.fromBytes(result);
        }

        // Replay cache: failed
        if (ctx.replayState.hasChildWorkflowFailed(childWorkflowId)) {
            String[] failure = ctx.replayState.getChildWorkflowFailure(childWorkflowId);
            throw new ChildWorkflowFailureException(childWorkflowId, failure[0], failure[1]);
        }

        // At the live edge — switch out of replay mode
        ctx.replayState.finishReplay();

        String effectiveTaskQueue = (options.taskQueue() != null) ? options.taskQueue() : ctx.taskQueue;
        byte[] inputBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);

        HistoryEvent.ChildWorkflowStarted priorStart =
                ctx.replayState.getChildWorkflowStarted(childWorkflowId);

        if (priorStart == null) {
            // Live execution: write ChildWorkflowStarted → parent history (records intent)
            LogTag parentHistoryTag = LogTag.of("workflow-history", ctx.workflowId);
            HistoryEvent.ChildWorkflowStarted childStartedEvent = new HistoryEvent.ChildWorkflowStarted(
                    UUID.randomUUID().toString(), Instant.now(),
                    ctx.workflowId, childWorkflowId, childWorkflowType, effectiveTaskQueue, inputBytes);
            ctx.sharedLog.append(
                    AppendRequest.to(parentHistoryTag,
                            HistorySerializer.INSTANCE.serialize(childStartedEvent))
            ).join();

            // Atomically write WorkflowStarted to child task-queue + child history
            writeChildWorkflowStarted(ctx.sharedLog, childWorkflowId, childWorkflowType,
                    effectiveTaskQueue, inputBytes);

            log.debug("Started child workflow {} (type={}, parent={})",
                    childWorkflowId, childWorkflowType, ctx.workflowId);
        } else {
            // Crash recovery: ChildWorkflowStarted in parent history, but child WorkflowStarted
            // may not have been written yet (crashed between the two writes). Re-write it —
            // WorkflowDispatcher handles duplicates via knownWorkflowIds.add().
            String recoveredTaskQueue = priorStart.taskQueue();
            writeChildWorkflowStarted(ctx.sharedLog, childWorkflowId, childWorkflowType,
                    recoveredTaskQueue, priorStart.input());

            log.debug("Recovered child workflow {} (type={}, parent={})",
                    childWorkflowId, childWorkflowType, ctx.workflowId);
        }

        // Start watcher virtual thread — subscribes to child history and forwards completion
        startChildWatcher(ctx, childWorkflowId);

        // Park the parent workflow virtual thread until completion is delivered
        byte[] resultBytes = ctx.awaitChildWorkflowResult(childWorkflowId);
        if (method.getReturnType() == Void.TYPE || resultBytes == null || resultBytes.length == 0) {
            return null;
        }
        return KryoSerializer.fromBytes(resultBytes);
    }

    private static void writeChildWorkflowStarted(SharedLog sharedLog, String childWorkflowId,
                                                   String childWorkflowType, String taskQueue,
                                                   byte[] inputBytes) {
        LogTag childTaskTag    = LogTag.of("workflow-tasks",    taskQueue);
        LogTag childHistoryTag = LogTag.of("workflow-history",  childWorkflowId);
        HistoryEvent.WorkflowStarted workflowStarted = new HistoryEvent.WorkflowStarted(
                UUID.randomUUID().toString(), Instant.now(),
                childWorkflowId, childWorkflowType, taskQueue, inputBytes);
        sharedLog.append(
                AppendRequest.to(Set.of(childTaskTag, childHistoryTag),
                        HistorySerializer.INSTANCE.serialize(workflowStarted))
        ).join();
    }

    /**
     * Subscribes to the child's history log. When {@code WorkflowCompleted} or
     * {@code WorkflowFailed} arrives, appends the corresponding
     * {@code ChildWorkflowCompleted}/{@code ChildWorkflowFailed} event to the parent history
     * so that {@code WorkflowRunner} can deliver it to the waiting parent virtual thread.
     */
    private static void startChildWatcher(WorkflowContext ctx, String childWorkflowId) {
        Thread.ofVirtual().name("boudin-child-watcher-" + childWorkflowId).start(() -> {
            LogTag childHistoryTag  = LogTag.of("workflow-history", childWorkflowId);
            LogTag parentHistoryTag = LogTag.of("workflow-history", ctx.workflowId);
            LogView childHistoryView = ctx.sharedLog.getView(childHistoryTag);
            CompletableFuture<Void> done = new CompletableFuture<>();

            SharedLog.Subscription sub = childHistoryView.subscribe(LogPosition.BEGINNING, entry -> {
                HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
                switch (event) {
                    case HistoryEvent.WorkflowCompleted wc -> {
                        HistoryEvent.ChildWorkflowCompleted completed = new HistoryEvent.ChildWorkflowCompleted(
                                UUID.randomUUID().toString(), Instant.now(),
                                ctx.workflowId, childWorkflowId, wc.result());
                        try {
                            ctx.sharedLog.append(
                                    AppendRequest.to(parentHistoryTag,
                                            HistorySerializer.INSTANCE.serialize(completed))
                            ).join();
                        } catch (Exception e) {
                            log.error("Failed to append ChildWorkflowCompleted for child {} to parent {}",
                                    childWorkflowId, ctx.workflowId, e);
                        }
                        done.complete(null);
                    }
                    case HistoryEvent.WorkflowFailed wf -> {
                        HistoryEvent.ChildWorkflowFailed failed = new HistoryEvent.ChildWorkflowFailed(
                                UUID.randomUUID().toString(), Instant.now(),
                                ctx.workflowId, childWorkflowId, wf.errorType(), wf.message());
                        try {
                            ctx.sharedLog.append(
                                    AppendRequest.to(parentHistoryTag,
                                            HistorySerializer.INSTANCE.serialize(failed))
                            ).join();
                        } catch (Exception e) {
                            log.error("Failed to append ChildWorkflowFailed for child {} to parent {}",
                                    childWorkflowId, ctx.workflowId, e);
                        }
                        done.complete(null);
                    }
                    default -> {}
                }
            });

            done.join();
            try { sub.close(); } catch (Exception ignored) {}
            log.debug("Child watcher for {} finished", childWorkflowId);
        });
    }
}
