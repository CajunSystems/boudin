# Phase 2, Plan 1 Summary: WorkflowDispatcher KV Recovery + WorkflowRunner Completion Callback

## Outcome

Both tasks completed. All 4 existing tests pass. No deviations from plan.

## Tasks Completed

| Task | Commit | Description |
|------|--------|-------------|
| 1 — WorkflowRunner onComplete | 698fa40 | Added `Runnable onComplete` as 7th constructor param; called after `close()` in both WorkflowCompleted and WorkflowFailed cases |
| 2 — WorkflowDispatcher KV active-set | 955495d | Rewrote `start()` with KV-backed active-workflow set; first startup falls back to full scan, subsequent startups use fast KV path |

## Key Changes

**WorkflowRunner.java** — Added `private final Runnable onComplete` field and 7th constructor parameter. Both `WorkflowCompleted` and `WorkflowFailed` branches in `onHistoryEvent` now call `if (onComplete != null) onComplete.run()` after `close()`.

**WorkflowDispatcher.java** — Full rewrite of the startup/recovery logic:
- Added `LogView taskView`, `Set<String> activeWorkflowIds`, `Object activeWorkflowsLock`, `KV_ACTIVE_WORKFLOWS` constant
- `start()` now has two paths: empty KV set → full scan + seed; non-empty KV set → fast recovery of known in-flight IDs only
- New `recoverWorkflow(String workflowId)` loads history for a specific workflow ID and starts a runner in replay mode, or removes from active set if already complete
- `handleWorkflowStarted()` calls `addToActiveSet()` before creating a runner, `removeFromActiveSet()` if workflow is already complete
- `onWorkflowComplete()` removes from runners map and active set — wired via the new `onComplete` callback
- `loadActiveWorkflows()` / `persistActiveWorkflows()` use `KryoSerializer` to read/write `HashSet<String>` from KV

## Verification

Log output confirms both paths work:
- First startup: `"no KV checkpoint, scanning full workflow-tasks log..."`
- Completion: `"workflow {id} removed from active set"`

## Deviations

None. Plan executed as written.

## Issues Found

None.
