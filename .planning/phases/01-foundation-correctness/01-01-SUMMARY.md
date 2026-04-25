# Phase 1, Plan 1 Summary: Foundation & Correctness

## Outcome

All 5 correctness fixes completed. 4 existing tests pass. No deviations from plan.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — Signal dedup | 2b281d1 | `WorkflowRunner` now tracks processed event seqnums; duplicate subscription delivery is a no-op |
| 2 — Activity map pruning | 8e270e8 | `WorkflowContext.awaitActivityResult` removes entry in `finally` after `join()` |
| 3 — Activity registration validation | 11bd87f | `ActivityRegistry.register` throws `IllegalArgumentException` if no `@ActivityInterface` found |
| 4 — Workflow registration validation | 8853f80 | `WorkflowRegistry.findAnnotatedMethod` counts matches and throws on duplicates; `register` throws on duplicate type |
| 5 — Test reliability | 0163e81 | Replaced `Thread.sleep(200)` with `Awaitility.await().until(WaitingWorkflowImpl.reachedAwait::get)` |

## Key Changes

**WorkflowRunner.java** — Added `Set<Long> processedEventSeqnums`. Subscription callback now passes `entry.seqnum()` to `onHistoryEvent`. Guard at top of method short-circuits duplicates.

**WorkflowContext.java** — `awaitActivityResult` wraps `future.join()` in try-finally; `pendingActivities.remove(activityId)` always runs after the workflow thread is unblocked.

**ActivityRegistry.java** — `foundInterface` boolean tracks whether any `@ActivityInterface` was seen; throws at end of loop if none found. Eliminates silent no-op registration.

**WorkflowRegistry.java** — `findAnnotatedMethod` streams `getDeclaredMethods()` into a list, throws descriptively on 0 or >1 matches. `register()` checks `workflows.containsKey` before inserting; throws on duplicate type name.

**GreetingWorkflowTest.java** — `WaitingWorkflowImpl` gains `static AtomicBoolean reachedAwait`; sets it to `true` at the start of `run()` before `Workflow.await()`. `setUp()` resets it to `false`. Test uses `await().atMost(5, SECONDS).until(reachedAwait::get)` instead of `Thread.sleep(200)`.

## Deviations

None. Plan executed as written.

## Issues Found

None new. All pre-existing issues in this scope are resolved.
