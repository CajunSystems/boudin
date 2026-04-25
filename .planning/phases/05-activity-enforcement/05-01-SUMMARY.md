# Phase 5, Plan 1: Retry Policy Expansion and Retry Loop — Summary

## Status: Complete

## What Was Built

Expanded `ActivityOptions` with `initialInterval`, `backoffCoefficient`, and `scheduleToStartTimeout`.
Added 5 retry policy fields to `ActivityScheduled` so the dispatcher carries everything it needs.
Replaced the single-attempt `executeActivity` with an exponential-backoff retry loop that appends
`ActivityFailed` with `errorType="maxAttemptsExceeded"` after exhausting all attempts.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — Expand ActivityOptions | 0ad2c5b | `initialInterval` (1s), `backoffCoefficient` (2.0), `scheduleToStartTimeout` (null) added to options and Builder |
| 2 — ActivityScheduled + ActivityStub | bb9fc1c | 5 new fields on record; ActivityStub populates from options |
| 3 — Retry loop in ActivityDispatcher | d184731 | Loop up to maxAttempts; `delay = initialIntervalMs × backoff^(attempt-1)`; terminal `ActivityFailed` |
| 4 — ActivityEnforcementTest + fix | 1395219 | 3 retry tests; fixed `CompletionException` unwrap in `awaitActivityResult` |

## Key Design Decisions

- **Policy fields travel with the event**: All retry parameters are embedded in `ActivityScheduled` so the dispatcher never needs a separate lookup or registry.
- **Backoff formula**: `delay(n) = initialIntervalMs × backoffCoefficient^(n-1)` — standard exponential backoff, compatible with Temporal's default behavior.
- **`backoffCoefficient=1.0` = constant interval**: Used in tests for deterministic timing.
- **`errorType="maxAttemptsExceeded"`**: Fixed string so workflow code can inspect it via `ActivityFailureException.errorType()`.

## Bug Fixed (Auto-fix)

`CompletionException` was wrapping `ActivityFailureException` inside `awaitActivityResult` because `CompletableFuture.join()` always wraps exceptions. Fixed by catching `CompletionException` and rethrowing the unwrapped `RuntimeException` cause so workflow code can `catch (ActivityFailureException e)` directly.

## Test Results

All 16 tests pass (`mvn test`):
- 13 pre-existing tests unchanged
- 3 new `ActivityEnforcementTest` tests: succeed-on-2nd-attempt, exhausted-retries, single-attempt-fail

## Issues / Deviations

Auto-fixed: `CompletionException` unwrap bug in `WorkflowContext.awaitActivityResult()` — not in the original plan but required for retry tests to work correctly. The fix is minimal and correct for all activity failure paths.
