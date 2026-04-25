# Phase 5, Plan 2: Timeout Enforcement — Summary

## Status: Complete

## What Was Built

Enforced `startToCloseTimeout` per activity attempt using `CompletableFuture.orTimeout()` and
enforced `scheduleToStartTimeout` by checking activity event age at dispatch time. Both
timeouts integrate with the retry loop from Plan 05-01 — they count as failed attempts and
trigger backoff before the next retry.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — startToCloseTimeout per attempt | 0dfd7b5 | `vtExecutor`, `invokeWithTimeout()` using `orTimeout()`; CompletionException/TimeoutException unwrapping; log distinguishes "timed out" vs "failed" |
| 2 — scheduleToStartTimeout dispatch check | df3eeba | Age check in `processActivityScheduled()` before spawning thread; `appendScheduleToStartFailure()` appends `ActivityFailed(scheduleToStartExceeded)` |
| 3 — ActivityTimeoutTest | 0828dc4 | Single-attempt timeout, all-3-attempts timeout, field propagation check |

## Key Design Decisions

- **`vtExecutor` for timeout isolation**: `CompletableFuture.supplyAsync(invoke, vtExecutor).orTimeout()` runs the activity on a separate virtual thread; the outer retry-loop thread blocks on the future. This cleanly interrupts the inner thread on timeout without touching the retry-loop thread.
- **Checked exception wrapping**: `activityRegistry.invoke()` declares `throws Exception`. The `Supplier` lambda wraps checked exceptions in `RuntimeException`; `invokeWithTimeout()` unwraps them before rethrowing, so callers see the original exception (not a double-wrapped one).
- **TimeoutException identity preserved**: `CompletionException(TimeoutException)` is re-thrown as-is; the outer catch block identifies it via `cause instanceof TimeoutException` to log "timed out" instead of "failed".
- **Schedule-to-start uses wall clock**: `Duration.between(as.timestamp(), Instant.now())` — works for both the startup scan (stale events) and live dispatch (nearly always within the window).

## Test Results

All 19 tests pass (`mvn test`):
- 16 pre-existing tests unchanged
- 3 new `ActivityTimeoutTest` tests: single-attempt timeout, three-attempt timeout, field propagation

## Issues / Deviations

None. Implementation matched plan exactly.
