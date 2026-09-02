# Phase 9, Plan 1: Query Methods — Summary

## Status: Complete (uncommitted — awaiting review)

## What Was Built

`@QueryMethod` works end-to-end. A client can read the state of a running workflow
synchronously, from the process that started it or from any process holding a `SharedLog`
reference.

Queries travel as a request/response pair on a dedicated `workflow-queries:{workflowId}` log
tag, using a new `QueryMessage` sealed hierarchy and `QuerySerializer` — separate from
`HistoryEvent` so query traffic never enters the replayed workflow history. `WorkflowRunner`
subscribes to that tag per workflow instance, resolves the handler through the existing
`WorkflowRegistry.findQueryMethod()`, and invokes it on the workflow implementation under
`WorkflowContext.signalLock` so a query never interleaves with a signal handler. Both
`WorkflowStub` and `SignalOnlyStub` route `@QueryMethod` calls through a shared `QueryClient`,
with failures surfacing as `WorkflowQueryException` carrying a distinguishable `errorType`.

## Tasks Completed

| Task | Description |
|------|-------------|
| 1 | `QueryMessage` (QueryRequested/Completed/Failed) + `QuerySerializer` in new `query` package |
| 2 | `WorkflowQueryException`; `WorkflowOptions.queryTimeout` with a 10s default |
| 3 | Registration validation — reject `void` query methods and duplicate query names |
| 4 | `WorkflowContext.invokeQuery` + `WorkflowRunner` query subscription and response append |
| 5 | `QueryClient` round trip wired into `WorkflowStub` and `SignalOnlyStub` |
| 6 | `QueryWorkflowTest` — 9 end-to-end tests |
| 7 | README "Queries" section; `QueryMethod` javadoc; fixed two non-existent client methods in docs |

## Key Design Decisions

- **Queries are not history.** `HistoryEvent`'s `permits` clause is untouched. A separate
  sealed `QueryMessage` hierarchy on a separate tag keeps replay, determinism, and history size
  unaffected — and makes it structurally hard to accidentally persist query traffic.
- **Correlated by per-request `queryId`.** The client subscribes before appending and filters
  responses by its own UUID, so concurrent queries against one workflow cannot cross wires. If
  two workers both answer (recovery overlap), the first response wins and the second is a no-op.
- **Handler runs under `signalLock`.** Gives mutual exclusion against signal delivery for free.
  Cost: a blocking query handler stalls signal delivery for that workflow — documented as a
  hard rule in both the annotation javadoc and the README.
- **Handler + append run on a virtual thread, not the event loop.** The response append blocks
  on `join()`; doing it on the event loop thread would stall dispatch for every other workflow
  on the worker. The event loop is used only for the `queryId` dedupe.
- **Running workflows only.** Once a workflow completes, its runner and query subscription are
  torn down, so queries against it fail with `QueryTimedOut`. Answering from terminal history is
  deliberately Phase 10's `WorkflowHandle.describe()`, not a query.
- **`WorkflowStub` now seeds `workflowId` from `WorkflowOptions`.** Previously a stub built with
  an explicit workflow ID could not signal or query until it had started the workflow itself.
  Needed for queries; also fixes that gap for signals.

## Test Results

- `QueryWorkflowTest` — 9/9 pass
- `mvn verify` — 37/37 pass (after the Phase 8.1 gumbo upgrade)

## Issues / Deviations

### Pre-existing full-suite hang (not caused by this plan) — since resolved

`mvn verify` hung indefinitely at `ChildWorkflowTest.childWorkflowFailurePropagatesToParent`.
Verified as pre-existing by reproducing it on a clean `git worktree` at `c492448` with none of
the Phase 9 changes applied. Root-caused to a lost-notification window in gumbo 0.2.0 and fixed
by upgrading to gumbo 0.6.0 — see `.planning/phases/08.1-gumbo-upgrade/08.1-01-SUMMARY.md`.

Worth noting for the query design: `QueryClient` subscribes-then-appends, exactly the pattern
that window swallowed. On 0.2.0 a query could have silently timed out when the worker's response
landed mid-backlog. The upgrade removes that exposure.

### Deviations from the plan

- Plan test case 2 ("query reflects a state change made by a signal") originally asserted a
  query *after* completion, which cannot work by design. Rewritten to use a non-unblocking
  `addNote` signal so the assertion happens while the workflow is still parked — this tests
  what the case was named for. A separate test now pins the completed-workflow behaviour
  explicitly (`QueryTimedOut`).
- Added a test beyond the plan asserting the history tag contains no `Query*` events — the
  central design invariant deserved direct coverage.
- Removed vestigial dead code in `WorkflowClient.newWorkflowStub(iface, workflowId, taskQueue)`
  (it built a `WorkflowStub`, called `getWorkflowId()` "to touch to verify", then discarded it).
- Fixed two documented-but-nonexistent client methods found while writing docs:
  `client.newSignalOnlyStub(...)` in the README and `client.signalWorkflow(...)` in the
  `WorkflowClient` javadoc.

### Known limitations to address later

- The `workflow-queries:{workflowId}` tag has no retention — a long-lived workflow queried
  frequently accumulates messages indefinitely. Related: the client subscribes from
  `BEGINNING`, so each query re-reads prior messages on that tag (correct, but O(n)).
- `WorkflowRunner.answeredQueryIds` grows for the lifetime of the workflow instance.
- Each `WorkflowRunner` now opens a second subscription. Worth revisiting as one query
  subscription per worker keyed by task queue if subscription count becomes a scaling concern.

## Output

Changes are staged in the working tree, not committed — the repo is on `main` and committing
was not requested. Suggested commit split:

- `feat(09-01): add QueryMessage sealed hierarchy and QuerySerializer`
- `feat(09-01): add WorkflowQueryException and WorkflowOptions.queryTimeout`
- `feat(09-01): validate query methods at workflow registration`
- `feat(09-01): handle query requests in WorkflowRunner and WorkflowContext`
- `feat(09-01): wire query round trip into WorkflowStub and SignalOnlyStub`
- `test(09-01): end-to-end query tests`
- `docs(09-01): document query methods in README`
