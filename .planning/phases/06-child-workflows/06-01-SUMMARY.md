# Phase 6, Plan 1: Child Workflow Data Model — Summary

## Status: Complete

## What Was Built

Three new sealed record types (`ChildWorkflowStarted`, `ChildWorkflowCompleted`, `ChildWorkflowFailed`) were added to the `HistoryEvent` hierarchy to capture child workflow lifecycle events in the parent's history log. Two new value objects (`ChildWorkflowOptions`, `ChildWorkflowFailureException`) were added to the `workflow` package as configuration and error contracts. `ReplayState` was extended with pre-scan caches and accessor methods for child workflow replay, plus a per-type sequence counter for deterministic child workflow ID generation.

## Tasks Completed

| Task | Commit   | Description |
|------|----------|-------------|
| 1    | cf51455  | Add ChildWorkflowStarted/Completed/Failed to HistoryEvent permits clause and body |
| 2    | 9ccfbbe  | Create ChildWorkflowOptions (record + builder) and ChildWorkflowFailureException |
| 3    | 62ccd25  | Extend ReplayState with child caches, 7 new accessor methods, updated replaying flag |

## Key Design Decisions

- `ChildWorkflowFailed` stores `errorType` and `message` as plain strings in the event record (not a Throwable), consistent with `WorkflowFailed` and `ActivityFailed`.
- `ReplayState.failedChildWorkflows` maps to `String[]` rather than `ChildWorkflowFailureException` to prevent a circular import between `internal` and `workflow` packages — the exception is constructed at the call site in `ChildWorkflowStub`.
- The `replaying` flag is set to true when either `completedChildWorkflows` or `failedChildWorkflows` is non-empty, matching the same logic used for activities and timers.
- `ChildWorkflowOptions` uses `null` for both `taskQueue` and `workflowId` to mean "inherit / auto-generate", with a `defaults()` factory for the common case.
- Child workflow IDs follow the scheme `{parentWorkflowId}:child:{childWorkflowType}:{seq}` driven by `nextChildWorkflowSequence()`, mirroring the activity sequence counter pattern.

## Test Results

All 19 tests pass (mvn test)

## Issues / Deviations

None — all tasks implemented exactly as specified in the plan.
