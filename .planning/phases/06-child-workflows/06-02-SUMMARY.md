# Phase 6, Plan 2: Child Workflow Runtime Wiring — Summary

## Status: Complete

## What Was Built

Extended `WorkflowContext` with a `pendingChildWorkflows` map and three coordination methods
that allow parent workflow virtual threads to park until child workflow results are delivered.
Added `ChildWorkflowStub` as an `InvocationHandler` with full live execution, replay caching,
and crash-recovery logic, plus `Workflow.newChildWorkflowStub()` as the typed public API.
`WorkflowRunner.onHistoryEvent()` now routes `ChildWorkflowCompleted` and `ChildWorkflowFailed`
events to unblock waiting parent threads.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — WorkflowContext coordination | 7509831 | Added pendingChildWorkflows ConcurrentHashMap and awaitChildWorkflowResult, deliverChildWorkflowResult, deliverChildWorkflowFailure methods using the same computeIfAbsent pattern as activity coordination |
| 2 — WorkflowRunner routing | aab2351 | Added ChildWorkflowCompleted and ChildWorkflowFailed cases to the onHistoryEvent() switch; switch now handles 8 event types explicitly |
| 3 — ChildWorkflowStub + Workflow API | a042dcf | Created ChildWorkflowStub with live/replay/crash-recovery logic and a watcher virtual thread; added Workflow.newChildWorkflowStub() with and without options |

## Key Design Decisions

- `ChildWorkflowStub` is package-private (no `public`) — exposed only via `Workflow.newChildWorkflowStub()`, matching the `ActivityStub` pattern
- Watcher virtual thread subscribes from `LogPosition.BEGINNING` on child history so it catches `WorkflowCompleted/Failed` even if the child finishes before the watcher starts
- Crash recovery re-writes `WorkflowStarted` to child task-queue + child history when `ChildWorkflowStarted` is in parent history but child is not yet done; `WorkflowDispatcher` deduplicates via `knownWorkflowIds`
- `ChildWorkflowStarted` is written to parent history first (records intent), then `WorkflowStarted` atomically to both child task-queue and child history — the dual-tag append mirrors the `WorkflowStub` pattern
- `ChildWorkflowFailureException` is imported explicitly in `WorkflowContext` even though it's in the same package, matching the plan specification

## Test Results

All 19 tests pass (`mvn test`)

## Issues / Deviations

None
