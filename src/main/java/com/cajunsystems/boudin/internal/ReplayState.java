package com.cajunsystems.boudin.internal;

import com.cajunsystems.boudin.history.HistoryEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks deterministic replay state for a workflow execution.
 *
 * <p>On crash recovery, a workflow's history is loaded from the log before the workflow
 * virtual thread is started. {@code ReplayState} pre-scans that history to build a cache
 * of activity results and timer outcomes, then hands results back deterministically as
 * the workflow code re-executes the same steps.
 *
 * <p>The key invariants:
 * <ul>
 *   <li>Activity IDs are generated deterministically using per-type sequence counters.
 *       The same workflow code with the same history will always generate the same IDs.
 *   <li>During replay, {@link #getActivityResult} returns cached results without
 *       blocking and without scheduling new activity tasks.
 *   <li>Once {@link #finishReplay()} is called, {@link #isReplaying()} returns false
 *       and the workflow enters live execution mode.
 * </ul>
 *
 * <p>Thread-safety: {@link #nextActivitySequence} may be called from the workflow
 * virtual thread only. {@link #finishReplay()} is also called from the workflow thread
 * (inside {@link com.cajunsystems.boudin.activity.ActivityStub}). Other methods are
 * read-only after construction and may be called from any thread.
 */
public class ReplayState {

    /** Pre-scanned map of activityId → result bytes from the loaded history. */
    private final Map<String, byte[]> completedActivities = new HashMap<>();

    /** Pre-scanned map of timerId → true (fired) from the loaded history. */
    private final Map<String, Boolean> firedTimers = new HashMap<>();

    /** Pre-scanned map of timerId → TimerStarted event (for in-progress crash recovery). */
    private final Map<String, HistoryEvent.TimerStarted> startedTimers = new HashMap<>();

    /** The seqnum of the last LogEntry in the loaded history (0 if history is empty). */
    private final long lastHistorySeqnum;

    /** Per-activity-type invocation counter for deterministic ID generation. */
    private final ConcurrentHashMap<String, AtomicInteger> activitySequences = new ConcurrentHashMap<>();

    /** Per-timer invocation counter for deterministic timer ID generation. */
    private final ConcurrentHashMap<String, AtomicInteger> timerSequences = new ConcurrentHashMap<>();

    /** Pre-scanned map of childWorkflowId → ChildWorkflowStarted (intent recorded, not yet done). */
    private final Map<String, HistoryEvent.ChildWorkflowStarted> startedChildWorkflows = new HashMap<>();

    /** Pre-scanned map of childWorkflowId → result bytes for completed child workflows. */
    private final Map<String, byte[]> completedChildWorkflows = new HashMap<>();

    /**
     * Pre-scanned map of childWorkflowId → [errorType, message] for failed child workflows.
     * String[] to avoid importing from the workflow package (circular dependency).
     */
    private final Map<String, String[]> failedChildWorkflows = new HashMap<>();

    /** Per-child-workflow-type invocation counter for deterministic child ID generation. */
    private final ConcurrentHashMap<String, AtomicInteger> childWorkflowSequences = new ConcurrentHashMap<>();

    /** True until {@link #finishReplay()} is called. */
    private volatile boolean replaying;

    /**
     * Constructs a {@code ReplayState} from the workflow's existing history.
     *
     * @param history the list of {@link HistoryEvent}s loaded from the log (may be empty)
     * @param lastHistorySeqnum the Gumbo seqnum of the last entry in history (0 if empty)
     */
    public ReplayState(List<HistoryEvent> history, long lastHistorySeqnum) {
        this.lastHistorySeqnum = lastHistorySeqnum;
        preloadResults(history);
        // Only replay if there are actual cached results to feed back — a workflow whose
        // history contains only WorkflowStarted (no completed activities or fired timers)
        // is effectively brand-new and must run live, not replay.
        this.replaying = !completedActivities.isEmpty() || !firedTimers.isEmpty()
                || !completedChildWorkflows.isEmpty() || !failedChildWorkflows.isEmpty();
    }

    private void preloadResults(List<HistoryEvent> history) {
        for (HistoryEvent event : history) {
            switch (event) {
                case HistoryEvent.ActivityCompleted ac ->
                        completedActivities.put(ac.activityId(), ac.result());
                case HistoryEvent.TimerStarted ts ->
                        startedTimers.put(ts.timerId(), ts);
                case HistoryEvent.TimerFired tf ->
                        firedTimers.put(tf.timerId(), true);
                case HistoryEvent.ChildWorkflowStarted cws ->
                        startedChildWorkflows.put(cws.childWorkflowId(), cws);
                case HistoryEvent.ChildWorkflowCompleted cwc -> {
                        startedChildWorkflows.remove(cwc.childWorkflowId()); // no longer pending
                        completedChildWorkflows.put(cwc.childWorkflowId(), cwc.result());
                }
                case HistoryEvent.ChildWorkflowFailed cwf -> {
                        startedChildWorkflows.remove(cwf.childWorkflowId()); // no longer pending
                        failedChildWorkflows.put(cwf.childWorkflowId(), new String[]{cwf.errorType(), cwf.message()});
                }
                default -> {} // other events not needed for replay cache
            }
        }
    }

    /**
     * Returns true while the workflow is replaying history.
     * Returns false after {@link #finishReplay()} has been called.
     */
    public boolean isReplaying() {
        return replaying;
    }

    /**
     * Switches the workflow out of replay mode into live execution.
     * Called by {@link com.cajunsystems.boudin.activity.ActivityStub} when an activity
     * is called that has no cached result (i.e., we've reached the live edge of history).
     */
    public void finishReplay() {
        this.replaying = false;
    }

    /**
     * Returns the cached result bytes for the given activity ID, or {@code null} if the
     * activity has not yet completed in the known history.
     *
     * <p>A null return during replay means we've reached the live edge — the activity
     * was scheduled but never completed before the crash.
     */
    public byte[] getActivityResult(String activityId) {
        return completedActivities.get(activityId);
    }

    /**
     * Returns true if the given activity ID has a completed result in the replay cache.
     */
    public boolean hasActivityResult(String activityId) {
        return completedActivities.containsKey(activityId);
    }

    /**
     * Returns true if the given timer ID fired in the replay history.
     */
    public boolean hasTimerFired(String timerId) {
        return firedTimers.getOrDefault(timerId, false);
    }

    /**
     * Returns the {@link HistoryEvent.TimerStarted} event for the given timer ID if it was
     * started in a prior execution but never fired (in-progress crash recovery).
     * Returns null if the timer was never started, or has already fired.
     */
    public HistoryEvent.TimerStarted getTimerStarted(String timerId) {
        if (firedTimers.containsKey(timerId)) return null; // already fired
        return startedTimers.get(timerId);
    }

    /**
     * Returns the Gumbo seqnum of the last entry in the loaded history.
     * Used by {@link WorkflowRunner} to subscribe to the history tag from this
     * position so that already-replayed events are not re-delivered.
     */
    public long lastHistorySeqnum() {
        return lastHistorySeqnum;
    }

    /**
     * Returns the next invocation sequence number for the given activity type.
     * Called by {@link com.cajunsystems.boudin.activity.ActivityStub} to generate
     * deterministic activity IDs.
     *
     * <p>Must only be called from the workflow virtual thread.
     */
    public int nextActivitySequence(String activityType) {
        return activitySequences
                .computeIfAbsent(activityType, k -> new AtomicInteger(0))
                .getAndIncrement();
    }

    /**
     * Returns the next invocation sequence number for timer creation.
     * Used to generate deterministic timer IDs inside workflows.
     */
    public int nextTimerSequence(String workflowId) {
        return timerSequences
                .computeIfAbsent(workflowId, k -> new AtomicInteger(0))
                .getAndIncrement();
    }

    public boolean hasChildWorkflowStarted(String childWorkflowId) {
        return startedChildWorkflows.containsKey(childWorkflowId);
    }

    public HistoryEvent.ChildWorkflowStarted getChildWorkflowStarted(String childWorkflowId) {
        return startedChildWorkflows.get(childWorkflowId);
    }

    public boolean hasChildWorkflowCompleted(String childWorkflowId) {
        return completedChildWorkflows.containsKey(childWorkflowId);
    }

    public byte[] getChildWorkflowResult(String childWorkflowId) {
        return completedChildWorkflows.get(childWorkflowId);
    }

    public boolean hasChildWorkflowFailed(String childWorkflowId) {
        return failedChildWorkflows.containsKey(childWorkflowId);
    }

    /** Returns [errorType, message] or null if not failed. */
    public String[] getChildWorkflowFailure(String childWorkflowId) {
        return failedChildWorkflows.get(childWorkflowId);
    }

    public int nextChildWorkflowSequence(String childWorkflowType) {
        return childWorkflowSequences
                .computeIfAbsent(childWorkflowType, k -> new AtomicInteger(0))
                .getAndIncrement();
    }
}
