# Phase 3, Plan 2 Summary: Route WorkflowRunner History Events Through Event Loop

## Outcome

All 3 tasks completed. All 10 tests pass. No deviations from plan.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — WorkflowDispatcher eventLoop param | bb67e6f | Added `BoudinEventLoop` as 4th constructor param; both runner construction sites pass it |
| 2 — WorkflowRunner subscription routing | c84006f | History subscription lambda submits to `eventLoop`; 8th constructor param added |
| 3 — Worker passes eventLoop | a048216 | `WorkflowDispatcher` construction in Worker now passes `eventLoop` as 4th arg |

## Key Changes

**WorkflowDispatcher.java** — Added `private final BoudinEventLoop eventLoop` field. Constructor updated to 4-arg form. Both `WorkflowRunner` instantiation sites in `recoverWorkflow()` and `handleWorkflowStarted()` pass `eventLoop` as the 8th argument.

**WorkflowRunner.java** — Added `private final BoudinEventLoop eventLoop` field and 8th constructor param. History subscription lambda now deserializes `entry.data()` on the Gumbo notification thread (lightweight), then calls `eventLoop.submit(() -> onHistoryEvent(...))`. All per-workflow event delivery (activity results, signals, timer fires, completion) is now serialized through the event loop thread.

**Worker.java** — One-line change: `WorkflowDispatcher` construction passes `eventLoop` as the 4th arg.

## Architecture Impact

After this plan, the threading model for WorkflowRunner events is:

```
Gumbo notification thread
  → deserialize entry (fast, stays on Gumbo thread)
  → eventLoop.submit(onHistoryEvent)
       → boudin-event-loop-{queue} thread
            → deliverActivityResult / deliverSignal / deliverTimerFired
                 → wakes workflow virtual thread
```

Phase 4 timer integration will fire `TimerFired` via `HashedWheelTimer.schedule()` which also
runs on the event loop thread. Delivery of that timer event through the history subscription
will also route through `eventLoop.submit()`, keeping the delivery path consistent and safe.

## Deviations

None. Plan executed as written.

## Issues Found

None.
