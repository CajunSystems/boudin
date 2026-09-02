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

    // ── Private ───────────────────────────────────────────────────────────────

    private Object startAndWait(Method method, Object[] args) throws Exception {
        String wfId = (options.workflowId() != null)
                ? options.workflowId()
                : UUID.randomUUID().toString();
        this.workflowId = wfId;

        String workflowType = workflowInterface.getSimpleName();
        LogTag taskTag = LogTag.of("workflow-tasks", options.taskQueue());
        LogTag historyTag = LogTag.of("workflow-history", wfId);
        LogView taskView = sharedLog.getView(taskTag);
        LogView historyView = sharedLog.getView(historyTag);

        // Subscribe BEFORE any append so we never miss WorkflowCompleted
        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();
        SharedLog.Subscription sub = historyView.subscribe(LogPosition.BEGINNING, entry -> {
            HistoryEvent event = HistorySerializer.INSTANCE.deserialize(entry.data());
            switch (event) {
                case HistoryEvent.WorkflowCompleted wc ->
                        resultFuture.complete(wc.result());
                case HistoryEvent.WorkflowFailed wf ->
                        resultFuture.completeExceptionally(
                                new WorkflowFailureException(wf.workflowId(), wf.errorType(), wf.message()));
                default -> {}
            }
        });

        // Idempotency check: if this workflowId was already submitted, skip re-append.
        // Note: narrow race window exists without CAS; acceptable for retry-based callers.
        byte[] alreadyStarted = taskView.getValue("wf-started:" + wfId).join();
        if (alreadyStarted == null) {
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
        } else {
            log.info("Workflow {} already started, waiting for completion", wfId);
        }

        try {
            byte[] resultBytes = resultFuture.join();
            if (method.getReturnType() == Void.TYPE || resultBytes == null || resultBytes.length == 0) {
                return null;
            }
            return KryoSerializer.fromBytes(resultBytes);
        } finally {
            try { sub.close(); } catch (Exception ignored) {}
        }
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
