package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.api.TypedLogView;
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
 *   <li>{@link QueryMethod}: Not yet implemented.</li>
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

    /** Set after the workflow is started; used by signal/query methods. */
    private volatile String workflowId;

    WorkflowStub(SharedLog sharedLog, Class<?> workflowInterface, WorkflowOptions options) {
        this.sharedLog = sharedLog;
        this.workflowInterface = workflowInterface;
        this.options = options;
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
            throw new UnsupportedOperationException(
                    "Query methods are not yet supported in Boudin v0.1");
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
        byte[] inputBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);

        HistoryEvent.WorkflowStarted startedEvent = new HistoryEvent.WorkflowStarted(
                UUID.randomUUID().toString(),
                Instant.now(),
                wfId,
                workflowType,
                options.taskQueue(),
                inputBytes
        );

        // Atomic dual-tag append: workflow task queue + workflow history
        LogTag taskTag = LogTag.of("workflow-tasks", options.taskQueue());
        LogTag historyTag = LogTag.of("workflow-history", wfId);

        log.info("Starting workflow {} (type={}, taskQueue={})",
                wfId, workflowType, options.taskQueue());

        sharedLog.append(
                AppendRequest.to(Set.of(taskTag, historyTag),
                        HistorySerializer.INSTANCE.serialize(startedEvent))
        ).join();

        // Subscribe to history and wait for completion
        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();

        TypedLogView<HistoryEvent> historyView =
                sharedLog.getTypedView(historyTag, HistorySerializer.INSTANCE);

        SharedLog.Subscription sub = historyView.subscribe(LogPosition.BEGINNING, event -> {
            switch (event) {
                case HistoryEvent.WorkflowCompleted wc ->
                        resultFuture.complete(wc.result());
                case HistoryEvent.WorkflowFailed wf ->
                        resultFuture.completeExceptionally(
                                new WorkflowFailureException(wf.workflowId(), wf.errorType(), wf.message()));
                default -> {}
            }
        });

        try {
            byte[] resultBytes = resultFuture.join(); // blocks caller until workflow completes
            if (method.getReturnType() == Void.TYPE || resultBytes == null || resultBytes.length == 0) {
                return null;
            }
            return KryoSerializer.fromBytes(resultBytes);
        } finally {
            try { sub.close(); } catch (Exception ignored) {}
        }
    }

    private Object sendSignal(Method method, Object[] args) throws Exception {
        if (workflowId == null) {
            throw new IllegalStateException(
                    "Cannot send signal: workflow has not been started yet. " +
                    "Call the @WorkflowMethod first.");
        }

        SignalMethod ann = method.getAnnotation(SignalMethod.class);
        String signalName = ann.name().isBlank() ? method.getName() : ann.name();

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
