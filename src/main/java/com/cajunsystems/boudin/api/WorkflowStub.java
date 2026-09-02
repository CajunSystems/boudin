package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.internal.WireNames;
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
 * {@link InvocationHandler} for client-side workflow stubs created by {@link WorkflowClient}.
 *
 * <p>Intercepts calls to workflow interface methods and routes them to the shared log:
 * <ul>
 *   <li>{@link WorkflowMethod}: Appends a {@link HistoryEvent.WorkflowStarted} event to
 *       both the task queue and the workflow's history tag, then subscribes to the history
 *       and blocks until {@link HistoryEvent.WorkflowCompleted} or
 *       {@link HistoryEvent.WorkflowFailed} arrives.</li>
 *   <li>{@link SignalMethod}: Appends a {@link HistoryEvent.SignalReceived} event to the
 *       workflow's history tag. Returns immediately.</li>
 *   <li>{@link QueryMethod}: Appends a {@code QueryRequested} to
 *       {@code workflow-queries:{workflowId}} and blocks until the worker running the
 *       workflow answers, or {@link WorkflowOptions#queryTimeout()} elapses. Query traffic
 *       never enters the workflow history.</li>
 * </ul>
 *
 * <p>The stub is <strong>not</strong> thread-safe — do not call signal methods
 * concurrently from multiple threads on the same stub.
 */
class WorkflowStub implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStub.class);

    private final SharedLog sharedLog;
    private final Class<?> workflowInterface;
    private final WorkflowOptions options;

    /**
     * The target workflow instance. Taken from {@link WorkflowOptions#workflowId()} when the
     * caller supplied one — so signals and queries work against a workflow this stub did not
     * start — otherwise assigned when the workflow is started.
     */
    private volatile String workflowId;

    WorkflowStub(SharedLog sharedLog, Class<?> workflowInterface, WorkflowOptions options) {
        this.sharedLog = sharedLog;
        this.workflowInterface = workflowInterface;
        this.options = options;
        this.workflowId = options.workflowId();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }

        if (method.isAnnotationPresent(WorkflowMethod.class)) {
            AsyncStart pending = ASYNC_START.get();
            if (pending != null) {
                // Inside WorkflowClient.start(...): durably start the workflow, record the ID for
                // the handle, and return without waiting for a terminal event.
                if (pending.workflowId != null) {
                    // start() can only hand back one handle, so a second workflow would run
                    // unobserved. Fail before durably starting it rather than after.
                    throw new IllegalStateException(
                            "WorkflowClient.start(...) started more than one workflow. Its lambda "
                            + "must call exactly one @WorkflowMethod — '" + pending.workflowId
                            + "' was already started, and only one handle can be returned. "
                            + "Call start(...) once per workflow.");
                }
                pending.workflowId = appendStart(args);
                pending.sharedLog = sharedLog;
                return defaultValueFor(method.getReturnType());
            }
            return startAndWait(method, args);
        } else if (method.isAnnotationPresent(SignalMethod.class)) {
            return sendSignal(method, args);
        } else if (method.isAnnotationPresent(QueryMethod.class)) {
            return sendQuery(method, args);
        }
        throw new UnsupportedOperationException("Unknown method: " + method.getName());
    }

    /** Returns the workflow ID after the workflow has been started, or null before that. */
    public String getWorkflowId() {
        return workflowId;
    }

    // ── Async start mode ──────────────────────────────────────────────────────

    /**
     * Marks the calling thread as being inside {@link WorkflowClient#start}, so a
     * {@code @WorkflowMethod} invocation starts the workflow and returns instead of waiting.
     *
     * <p>A thread-local is what lets a plain typed method call express "start this" — the user
     * writes {@code client.start(() -> stub.process(order))} and the compiler checks the
     * arguments, with no per-arity functional interfaces.
     */
    private static final ThreadLocal<AsyncStart> ASYNC_START = new ThreadLocal<>();

    /** Carries the started workflow ID, and the log it was started on, out of the user's lambda. */
    static final class AsyncStart {
        String workflowId;
        SharedLog sharedLog;
    }

    static AsyncStart beginAsyncStart() {
        if (ASYNC_START.get() != null) {
            // A nested start would overwrite the outer thread-local and then clear it on the way
            // out, silently dropping the outer lambda back into blocking start.
            throw new IllegalStateException(
                    "WorkflowClient.start(...) cannot be nested inside another start(...). "
                    + "Start each workflow with its own top-level call.");
        }
        AsyncStart pending = new AsyncStart();
        ASYNC_START.set(pending);
        return pending;
    }

    static void endAsyncStart() {
        ASYNC_START.remove();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private Object startAndWait(Method method, Object[] args) throws Exception {
        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();

        // With a supplied workflow ID we can subscribe before appending. With a generated one the
        // history tag is not known until the append returns — subscribing from BEGINNING
        // afterwards is equivalent, since the backlog carries any terminal event already written.
        SharedLog.Subscription sub = options.workflowId() != null
                ? subscribeForResult(options.workflowId(), resultFuture)
                : null;

        // appendStart can throw — an unreachable log backend fails inside its join() calls — so
        // it belongs inside the try that closes the subscription. Otherwise every failed or
        // retried start with a supplied workflow ID leaked one subscription.
        try {
            String wfId = appendStart(args);
            if (sub == null) {
                sub = subscribeForResult(wfId, resultFuture);
            }

            byte[] resultBytes = resultFuture.join();
            if (method.getReturnType() == Void.TYPE || resultBytes == null || resultBytes.length == 0) {
                return null;
            }
            return KryoSerializer.fromBytes(resultBytes);
        } finally {
            // Still null when the ID was generated and appendStart threw before we could
            // subscribe — the very path this try exists to cover.
            if (sub != null) {
                try { sub.close(); } catch (Exception ignored) {}
            }
        }
    }

    private SharedLog.Subscription subscribeForResult(String wfId,
                                                      CompletableFuture<byte[]> resultFuture) {
        return sharedLog.getView(LogTag.of("workflow-history", wfId))
                .subscribe(LogPosition.BEGINNING,
                        entry -> TerminalEvents.completeFrom(entry, resultFuture, wfId));
    }

    /**
     * Durably starts the workflow and returns its ID, doing nothing if this workflow ID was
     * already started.
     *
     * <p>Shared by blocking and async start so the two cannot drift apart on the idempotency
     * check — the property that makes a retrying caller safe.
     */
    private String appendStart(Object[] args) {
        String wfId = (options.workflowId() != null)
                ? options.workflowId()
                : UUID.randomUUID().toString();
        this.workflowId = wfId;

        String workflowType = workflowInterface.getSimpleName();
        LogTag taskTag = LogTag.of("workflow-tasks", options.taskQueue());
        LogTag historyTag = LogTag.of("workflow-history", wfId);
        LogView taskView = sharedLog.getView(taskTag);

        // Idempotency check: if this workflowId was already submitted, skip re-append.
        // Note: narrow race window exists without CAS; acceptable for retry-based callers.
        byte[] alreadyStarted = taskView.getValue("wf-started:" + wfId).join();
        if (alreadyStarted != null) {
            log.info("Workflow {} already started", wfId);
            return wfId;
        }

        byte[] inputBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);
        HistoryEvent.WorkflowStarted startedEvent = new HistoryEvent.WorkflowStarted(
                UUID.randomUUID().toString(),
                Instant.now(),
                wfId,
                workflowType,
                options.taskQueue(),
                inputBytes
        );

        log.info("Starting workflow {} (type={}, taskQueue={})", wfId, workflowType, options.taskQueue());

        sharedLog.append(
                AppendRequest.to(Set.of(taskTag, historyTag),
                        HistorySerializer.INSTANCE.serialize(startedEvent))
        ).join();

        taskView.setValue("wf-started:" + wfId, new byte[0]).join();
        return wfId;
    }

    /**
     * The value an async start returns from the workflow method.
     *
     * <p>Must be type-appropriate, not simply {@code null}: a workflow method declared to return
     * {@code int} would have a {@code null} unboxed at the lambda's return, throwing a
     * {@link NullPointerException} nowhere near its cause.
     */
    private static Object defaultValueFor(Class<?> returnType) {
        if (!returnType.isPrimitive() || returnType == Void.TYPE) return null;
        if (returnType == boolean.class) return false;
        if (returnType == char.class)    return '\0';
        if (returnType == byte.class)    return (byte) 0;
        if (returnType == short.class)   return (short) 0;
        if (returnType == int.class)     return 0;
        if (returnType == long.class)    return 0L;
        if (returnType == float.class)   return 0f;
        if (returnType == double.class)  return 0d;
        return null;
    }

    private Object sendQuery(Method method, Object[] args) {
        if (workflowId == null) {
            throw new IllegalStateException(
                    "Cannot send query: workflow has not been started yet. " +
                    "Call the @WorkflowMethod first, or use " +
                    "WorkflowClient.newWorkflowStub(interface, workflowId, taskQueue) " +
                    "to target a workflow started elsewhere.");
        }
        return QueryClient.query(sharedLog, workflowId, method, args, options.queryTimeout());
    }

    private Object sendSignal(Method method, Object[] args) throws Exception {
        if (workflowId == null) {
            throw new IllegalStateException(
                    "Cannot send signal: workflow has not been started yet. " +
                    "Call the @WorkflowMethod first.");
        }

        String signalName = WireNames.signalName(method);

        byte[] payloadBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);

        HistoryEvent.SignalReceived signalEvent = new HistoryEvent.SignalReceived(
                UUID.randomUUID().toString(),
                Instant.now(),
                workflowId,
                signalName,
                payloadBytes
        );

        LogTag historyTag = LogTag.of("workflow-history", workflowId);
        sharedLog.append(
                AppendRequest.to(historyTag, HistorySerializer.INSTANCE.serialize(signalEvent))
        ).join();

        log.debug("Signal '{}' sent to workflow {}", signalName, workflowId);
        return null;
    }
}
