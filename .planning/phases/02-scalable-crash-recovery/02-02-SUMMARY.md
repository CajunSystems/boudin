# Phase 2, Plan 2 Summary: ActivityDispatcher Checkpoint + Idempotent Start + Tests

## Outcome

All 3 tasks completed. 6 tests pass (4 original + 2 new). No deviations from plan.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — ActivityDispatcher checkpoint | 8656e5b | KV seqnum checkpoint on activity-tasks:{queue}; readAfter(checkpoint) on restart |
| 2 — WorkflowStub idempotent start | 1cd6edf | KV flag "wf-started:{workflowId}" checked before appending WorkflowStarted |
| 3 — CrashRecoveryTest | 84dcf2e | Two integration tests verifying restart safety and idempotent start |

## Key Changes

**ActivityDispatcher.java** — Added `LogView taskView` field, `KV_CHECKPOINT` constant, `readCheckpoint()` and `saveCheckpoint()` helpers. `start()` now reads the checkpoint seqnum, uses `readAfter(checkpoint)` when non-zero, updates checkpoint after scan and after each live event.

**WorkflowStub.java** — `startAndWait()` restructured: subscription created first, then KV check for `"wf-started:{wfId}"`, then conditional append + KV flag set. Second call with same workflowId logs "already started" and waits for the history subscription to deliver the existing WorkflowCompleted.

**CrashRecoveryTest.java** — New test class with two tests:
- `workerRestartDoesNotReExecuteCompletedWorkflows`: shares `InMemoryPersistenceAdapter` between worker1 and worker2; verifies `callCount` stays at 1 after restart
- `idempotentWorkflowStartDoesNotExecuteTwice`: submits same `workflowId` twice; verifies `callCount` stays at 1

## Verification

Log output confirms expected behavior:
- `workerRestartDoesNotReExecuteCompletedWorkflows`: second worker logs `"no KV checkpoint, scanning full workflow-tasks log..."` and then `"workflow ... is already complete, skipping"` — 0 activities re-executed
- `idempotentWorkflowStartDoesNotExecuteTwice`: second call logs `"Workflow idempotent-test-workflow already started, waiting for completion"`
- Activity checkpoint: `"scanning activity tasks from seqnum 1 (checkpoint)..."` on second worker startup

## Deviations

None. Plan executed as written.

## Issues Found

None.
