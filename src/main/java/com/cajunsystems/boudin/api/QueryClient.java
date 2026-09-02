package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.annotation.QueryMethod;
import com.cajunsystems.boudin.internal.WireNames;
import com.cajunsystems.boudin.query.QueryMessage;
import com.cajunsystems.boudin.query.QuerySerializer;
import com.cajunsystems.boudin.serialization.KryoSerializer;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Client half of the query protocol, shared by {@link WorkflowStub} and {@link SignalOnlyStub}.
 *
 * <p>Only a {@link SharedLog} reference is needed — queries work from a process that has no
 * worker and never started the workflow.
 *
 * @see com.cajunsystems.boudin.query.QueryMessage
 */
final class QueryClient {

    private static final Logger log = LoggerFactory.getLogger(QueryClient.class);

    private QueryClient() {}

    /**
     * Sends a query to a running workflow and blocks until the worker answers.
     *
     * @param sharedLog  the shared log
     * @param workflowId the target workflow instance
     * @param method     the {@link QueryMethod}-annotated interface method being invoked
     * @param args       the call arguments, or null for none
     * @param timeout    how long to wait for an answer
     * @return the deserialized handler return value, or null for an empty result
     * @throws WorkflowQueryException if the query is unknown, the handler threw, or nothing
     *                                answered within {@code timeout}
     */
    static Object query(SharedLog sharedLog, String workflowId, Method method,
                        Object[] args, Duration timeout) {

        String queryName = WireNames.queryName(method);
        String queryId = UUID.randomUUID().toString();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        LogTag queryTag = LogTag.of("workflow-queries", workflowId);
        LogView queryView = sharedLog.getView(queryTag);

        // Subscribe BEFORE appending so a fast worker's response is never missed — but from the
        // tag's current tip, not BEGINNING. The response necessarily lands after our own
        // request, which lands after this subscription, so nothing relevant is upstream of here;
        // reading from BEGINNING made every query deserialize all prior traffic on the tag.
        CompletableFuture<QueryMessage> responseFuture = new CompletableFuture<>();
        SharedLog.Subscription sub = queryView.subscribe(
                new LogPosition(queryView.getLatestSeqnum() + 1),
                entry -> {
                    QueryMessage message;
                    try {
                        message = QuerySerializer.INSTANCE.deserialize(entry.data());
                    } catch (Exception e) {
                        // An entry this build cannot read (e.g. a client on a newer version)
                        // must not stop us seeing our own response further down the tag.
                        log.warn("Skipping undeserializable entry at seqnum {} on {}: {}",
                                entry.seqnum(), queryTag, e.toString());
                        return;
                    }
                    // Filter on our own queryId — concurrent queries share this tag.
                    if (!queryId.equals(message.queryId())) return;
                    switch (message) {
                        case QueryMessage.QueryCompleted qc -> responseFuture.complete(qc);
                        case QueryMessage.QueryFailed qf -> responseFuture.complete(qf);
                        case QueryMessage.QueryRequested ignored -> { }
                    }
                }
        );

        try {
            byte[] argBytes = KryoSerializer.toBytes(args != null ? args : new Object[0]);
            QueryMessage.QueryRequested request = new QueryMessage.QueryRequested(
                    queryId, Instant.now(), workflowId, queryName, argBytes);

            // Timed, and charged against the same budget as the wait below. An unbounded join()
            // here meant an unreachable log backend hung the caller forever, however short
            // queryTimeout was.
            awaitWithin(sharedLog.append(AppendRequest.to(
                            queryTag, QuerySerializer.INSTANCE.serialize(request))),
                    deadlineNanos, workflowId, queryName,
                    "Timed out appending the query request after " + timeout
                            + " — the shared log may be unreachable.");

            log.debug("Query '{}' sent to workflow {} (queryId={})", queryName, workflowId, queryId);

            QueryMessage response = awaitWithin(responseFuture, deadlineNanos, workflowId, queryName,
                    "No worker answered within " + timeout + ". The workflow may have "
                            + "completed, or no worker is running it on its task queue.");

            if (response instanceof QueryMessage.QueryFailed failed) {
                throw new WorkflowQueryException(
                        workflowId, queryName, failed.errorType(), failed.message());
            }

            byte[] resultBytes = ((QueryMessage.QueryCompleted) response).result();
            if (resultBytes == null || resultBytes.length == 0) return null;
            return KryoSerializer.fromBytes(resultBytes);

        } finally {
            try { sub.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Waits on {@code future} until {@code deadlineNanos}, translating every outcome into a
     * {@link WorkflowQueryException} so one budget covers the whole round trip.
     */
    private static <T> T awaitWithin(CompletableFuture<T> future, long deadlineNanos,
                                     String workflowId, String queryName, String timeoutMessage) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            throw new WorkflowQueryException(workflowId, queryName, "QueryTimedOut", timeoutMessage);
        }
        try {
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new WorkflowQueryException(workflowId, queryName, "QueryTimedOut", timeoutMessage);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WorkflowQueryException(workflowId, queryName, "QueryInterrupted",
                    "Interrupted while waiting for a query response.");
        } catch (ExecutionException e) {
            throw new WorkflowQueryException(workflowId, queryName, "QueryFailed",
                    String.valueOf(e.getCause()));
        }
    }
}
