package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.annotation.SignalMethod;
import com.cajunsystems.boudin.annotation.WorkflowMethod;
import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.internal.WireNames;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.UUID;

/**
 * {@link InvocationHandler} for stubs that target an already-running workflow by ID.
 *
 * <p>Used by {@link WorkflowClient#newWorkflowStub(Class, String, String)} to send signals to,
 * and run queries against, a workflow identified by its ID without starting a new execution.
 * Calling the {@code @WorkflowMethod} is rejected.
 *
 * <p>Queries use {@link WorkflowOptions#DEFAULT_QUERY_TIMEOUT}. To choose a different timeout,
 * use {@link WorkflowClient#newWorkflowStub(Class, WorkflowOptions)} with both
 * {@code workflowId} and {@code queryTimeout} set.
 */
class SignalOnlyStub implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(SignalOnlyStub.class);

    private final SharedLog sharedLog;
    private final Class<?> workflowInterface;
    private final String workflowId;

    SignalOnlyStub(SharedLog sharedLog, Class<?> workflowInterface, String workflowId) {
        this.sharedLog = sharedLog;
        this.workflowInterface = workflowInterface;
        this.workflowId = workflowId;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(this, args);
        }
        if (method.isAnnotationPresent(WorkflowMethod.class)) {
            throw new UnsupportedOperationException(
                    "Cannot call @WorkflowMethod on a signal-only stub. " +
                    "Use WorkflowClient.newWorkflowStub(interface, options) to start workflows.");
        }
        if (method.isAnnotationPresent(SignalMethod.class)) {
            return sendSignal(method, args);
        }
        if (method.isAnnotationPresent(QueryMethod.class)) {
            return QueryClient.query(
                    sharedLog, workflowId, method, args, WorkflowOptions.DEFAULT_QUERY_TIMEOUT);
        }
        throw new UnsupportedOperationException("Unknown method: " + method.getName());
    }

    private Object sendSignal(Method method, Object[] args) throws Exception {
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
