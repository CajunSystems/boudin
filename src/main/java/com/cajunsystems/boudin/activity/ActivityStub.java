package com.cajunsystems.boudin.activity;

import com.cajunsystems.boudin.annotation.ActivityMethod;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.internal.ActivityRegistry;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.boudin.workflow.Workflow;
import com.cajunsystems.boudin.workflow.WorkflowContext;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * {@link InvocationHandler} that intercepts activity calls on a workflow's activity proxy.
 *
 * <h2>Normal execution</h2>
 * <ol>
 *   <li>Generates a deterministic {@code activityId} using the activity type name and a
 *       per-type sequence counter from {@link com.cajunsystems.boudin.internal.ReplayState}.</li>
 *   <li>Serializes the method arguments via {@link KryoSerializer}.</li>
 *   <li>Appends an {@link HistoryEvent.ActivityScheduled} to <em>both</em>
 *       {@code workflow-history:{workflowId}} and {@code activity-tasks:{taskQueue}}
 *       in one atomic log append (multi-tag). This ensures the task is visible to activity
 *       workers and is durably recorded in the history in a single operation.</li>
 *   <li>Parks the workflow virtual thread on {@link WorkflowContext#awaitActivityResult}
 *       until the activity completes.</li>
 *   <li>Deserializes the result bytes and returns the typed value.</li>
 * </ol>
 *
 * <h2>Replay mode</h2>
 * If {@link com.cajunsystems.boudin.internal.ReplayState#isReplaying()} is true and the
 * activity result is in the replay cache ({@link com.cajunsystems.boudin.internal.ReplayState#getActivityResult}),
 * the cached result is returned immediately — <strong>no new log entry is written and
 * the workflow thread does not block</strong>. This is the deterministic replay guarantee.
 *
 * <p>If replaying but the result is not in the cache, we've reached the live edge of the
 * history (the activity was scheduled but never completed before the crash).
 * {@link com.cajunsystems.boudin.internal.ReplayState#finishReplay()} is called and
 * normal live execution resumes from this point.
 */
public class ActivityStub implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(ActivityStub.class);

    private final Class<?> activityInterface;
    private final ActivityOptions options;

    public ActivityStub(Class<?> activityInterface, ActivityOptions options) {
        this.activityInterface = activityInterface;
        this.options = options;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Delegate Object methods (toString, equals, hashCode) to this handler
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }

        if (!method.isAnnotationPresent(ActivityMethod.class)) {
            throw new IllegalStateException(
                    "Method " + method.getName() + " is not annotated with @ActivityMethod");
        }

        // Resolve context lazily — safe whether stub was created in a field initializer
        // or inside the workflow method body.
        WorkflowContext context = Workflow.currentContext();

        String activityType = ActivityRegistry.activityTypeKey(activityInterface, method);
        int seq = context.replayState.nextActivitySequence(activityType);
        String activityId = context.workflowId + ":" + activityType + ":" + seq;

        // ── Replay mode: return cached result without scheduling ──────────────
        if (context.replayState.isReplaying()) {
            if (context.replayState.hasActivityResult(activityId)) {
                byte[] cached = context.replayState.getActivityResult(activityId);
                log.debug("Replay: returning cached result for activity {} in workflow {}",
                        activityId, context.workflowId);
                return deserializeResult(method, cached);
            } else {
                // Reached the live edge — switch to live execution from here
                log.debug("Replay: live edge reached at activity {} in workflow {}",
                        activityId, context.workflowId);
                context.replayState.finishReplay();
            }
        }

        // ── Live mode: schedule the activity via the log ──────────────────────
        String targetTaskQueue = (options.taskQueue() != null)
                ? options.taskQueue()
                : context.taskQueue;

        byte[] inputBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);

        HistoryEvent.ActivityScheduled scheduled = new HistoryEvent.ActivityScheduled(
                UUID.randomUUID().toString(),
                Instant.now(),
                context.workflowId,
                activityId,
                activityType,
                targetTaskQueue,
                inputBytes
        );

        // Atomic dual-tag append: workflow history + activity task queue
        LogTag historyTag = LogTag.of("workflow-history", context.workflowId);
        LogTag taskTag = LogTag.of("activity-tasks", targetTaskQueue);

        log.debug("Scheduling activity {} in workflow {}", activityId, context.workflowId);
        context.sharedLog.append(
                AppendRequest.to(Set.of(historyTag, taskTag), HistorySerializer.INSTANCE.serialize(scheduled))
        ).join();

        // Block the workflow virtual thread until the result arrives
        byte[] resultBytes = context.awaitActivityResult(activityId);
        return deserializeResult(method, resultBytes);
    }

    private Object deserializeResult(Method method, byte[] resultBytes) {
        if (method.getReturnType() == Void.TYPE || resultBytes == null || resultBytes.length == 0) {
            return null;
        }
        return KryoSerializer.fromBytes(resultBytes);
    }
}
