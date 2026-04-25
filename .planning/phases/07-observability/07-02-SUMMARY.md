# Phase 7, Plan 2: MDC Context Propagation and Observability Tests — Summary

## Status: Complete

## What Was Built

SLF4J MDC context propagation was added to `WorkflowThread.run()` (setting `workflowId` and `workflowType`) and `ActivityDispatcher.executeActivity()` (setting `activityId` and `activityType`), so all log statements during workflow and activity execution automatically carry those fields. Four integration tests in `ObservabilityTest` verify that all Micrometer metrics introduced in 07-01 are correctly recorded end-to-end using `SimpleMeterRegistry`.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| Task 1 — MDC propagation | `68a8305` | Added `MDC.put/remove` for `workflowId`/`workflowType` in `WorkflowThread.run()` and `activityId`/`activityType` in `ActivityDispatcher.executeActivity()` |
| Task 2 — ObservabilityTest | `05253f0` | Created `ObservabilityTest.java` with 4 tests covering workflow completion, failure, activity completion, and pending gauge |

## Key Design Decisions

- MDC keys are set immediately after thread-local context installation and cleared in the same `finally` block — guaranteeing cleanup even on exceptions
- `SimpleMeterRegistry` is injected via `Worker.newBuilder().metricsRegistry(registry)` — tests are fully isolated, no shared global state
- `registry.counter(name, tags...).count()` returns 0.0 for unregistered meters, so "expected zero" assertions work without meter existence checks
- The `boudin.worker.pending_workflows` gauge is tested after workflow completion (synchronous stub call ensures completion before assertion)
- `assertThatThrownBy(stub::run).isInstanceOf(Exception.class)` used for the failing workflow test — broad enough to cover `CompletionException(WorkflowFailureException)` wrapping

## Test Results

All 27 tests pass (mvn test) — 23 pre-existing + 4 new ObservabilityTest

## Issues / Deviations

None — implementation matched the plan exactly. All 4 ObservabilityTest cases passed on the first run without any auto-fixes required.
