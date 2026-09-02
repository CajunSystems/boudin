package com.cajunsystems.boudin.query;

import java.time.Instant;

/**
 * Sealed hierarchy of control-plane messages exchanged on the
 * {@code workflow-queries:{workflowId}} log tag.
 *
 * <p>Queries are deliberately <strong>not</strong>
 * {@link com.cajunsystems.boudin.history.HistoryEvent}s. The workflow history is replayed on
 * every crash recovery and pre-scanned by {@link com.cajunsystems.boudin.internal.ReplayState};
 * query traffic is read-only, must not affect determinism, and must not grow the history. It
 * therefore lives on its own tag with its own serializer.
 *
 * <h2>Protocol</h2>
 * <ol>
 *   <li>A client subscribes to {@code workflow-queries:{workflowId}} and appends a
 *       {@link QueryRequested} carrying a fresh {@code queryId}.</li>
 *   <li>The worker running the workflow resolves the {@code @QueryMethod} handler, invokes it
 *       on the workflow implementation, and appends {@link QueryCompleted} or
 *       {@link QueryFailed} carrying the same {@code queryId}.</li>
 *   <li>The client matches on {@code queryId} — concurrent queries to one workflow do not
 *       collide — and closes its subscription.</li>
 * </ol>
 */
public sealed interface QueryMessage
        permits QueryMessage.QueryRequested,
                QueryMessage.QueryCompleted,
                QueryMessage.QueryFailed {

    /** Correlation ID shared by a request and its response. */
    String queryId();

    Instant timestamp();

    String workflowId();

    /**
     * A client is asking a running workflow for its current state.
     */
    record QueryRequested(
            String queryId,
            Instant timestamp,
            String workflowId,
            String queryName,
            byte[] args          // KryoSerializer.toBytes(Object[] args)
    ) implements QueryMessage {}

    /**
     * The query handler returned normally.
     */
    record QueryCompleted(
            String queryId,
            Instant timestamp,
            String workflowId,
            byte[] result        // KryoSerializer.toBytes(returnValue); empty for null
    ) implements QueryMessage {}

    /**
     * The query could not be answered — unknown query name, or the handler threw.
     */
    record QueryFailed(
            String queryId,
            Instant timestamp,
            String workflowId,
            String errorType,
            String message
    ) implements QueryMessage {}
}
