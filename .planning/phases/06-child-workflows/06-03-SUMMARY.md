# Phase 6, Plan 3: Child Workflow Tests — Summary

## Status: Complete

## What Was Built

Four integration tests in `ChildWorkflowTest.java` verifying end-to-end child workflow behavior: single
parent-to-child invocation, sequential child calls on the same stub, child failure propagation via
`ChildWorkflowFailureException`, and worker restart idempotency confirming completed workflows are not
re-executed after a worker bounce.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — Happy path tests | d14fe15 | parentInvokesChildWorkflow and parentInvokesSequentialChildren |
| 2 — Failure and idempotency tests | d14fe15 | childWorkflowFailurePropagatesToParent and workerRestartIsIdempotentAfterChildCompletes (included in same commit) |

## Key Design Decisions

- All 4 tests written together in one implementation pass since the fixtures and patterns were
  well-understood from the plan; both task commits map to the single `d14fe15` commit.
- The `errorType` in `ChildWorkflowFailureException` is `cause.getClass().getName()` (from
  `WorkflowThread.appendFailed`), so the failure test asserts `"child-failed:java.lang.RuntimeException"`.
- The sequential children test expected value was corrected from the plan's `"Hello, Alice!|Hello, Alice!2!"`
  to the actual `"Hello, Alice!|Hello, Alice2!"` — the second child receives `"Alice2"` as input and
  returns `"Hello, Alice2!"` (no extra `!` before `2`).
- The worker restart test uses the same `InMemoryPersistenceAdapter`-backed `SharedLogService` across
  two `Worker` instances, verifying that completed workflows are skipped during startup recovery.

## Test Results

All 23 tests pass (mvn test) — 19 pre-existing + 4 new ChildWorkflowTest

## Issues / Deviations

- Plan expected value for sequential children was `"Hello, Alice!|Hello, Alice!2!"` but actual
  behavior produces `"Hello, Alice!|Hello, Alice2!"`. The plan description had an error in the
  expected string — auto-fixed by running the test and correcting the assertion to match actual behavior.
- Both task commits were combined into one commit (`d14fe15`) since all 4 tests were written in a
  single implementation pass; the plan called for two separate commits.
