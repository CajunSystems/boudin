package com.cajunsystems.boudin.api;

import com.cajunsystems.boudin.history.HistoryEvent;
import com.cajunsystems.boudin.history.HistorySerializer;
import com.cajunsystems.gumbo.core.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The single definition of "this history entry ends the workflow".
 *
 * <p>Both ways of waiting for a result — a blocking {@link WorkflowStub} call and a
 * {@link WorkflowHandle} — need to decide the same thing from the same bytes. Two copies of this
 * would have to evolve in lockstep: adding a terminal event or changing how
 * {@link WorkflowFailureException} is built in one and not the other would make blocking stubs
 * and handles disagree about when a workflow has finished.
 */
final class TerminalEvents {

    private static final Logger log = LoggerFactory.getLogger(TerminalEvents.class);

    private TerminalEvents() {}

    /**
     * Completes {@code resultFuture} if {@code entry} is a terminal event, and does nothing
     * otherwise.
     *
     * <p>An entry this build cannot deserialize is logged and skipped rather than thrown: a
     * terminal event may lie further along the history, and failing here would strand the waiter.
     *
     * @param entry        the history entry to inspect
     * @param resultFuture completed with the result bytes, or exceptionally with
     *                     {@link WorkflowFailureException}
     * @param workflowId   for log context only
     */
    static void completeFrom(LogEntry entry,
                             CompletableFuture<byte[]> resultFuture,
                             String workflowId) {
        HistoryEvent event;
        try {
            event = HistorySerializer.INSTANCE.deserialize(entry.data());
        } catch (Exception e) {
            log.warn("Skipping undeserializable history entry at seqnum {} for workflow {}: {}",
                    entry.seqnum(), workflowId, e.toString());
            return;
        }
        switch (event) {
            case HistoryEvent.WorkflowCompleted completed ->
                    resultFuture.complete(completed.result());
            case HistoryEvent.WorkflowFailed failed ->
                    resultFuture.completeExceptionally(new WorkflowFailureException(
                            failed.workflowId(), failed.errorType(), failed.message()));
            default -> { }
        }
    }
}
