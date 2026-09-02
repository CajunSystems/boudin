# Phase 9, Plan 1: Query Methods — Summary

## Status: Merged — PR #3, merge commit `7c4f804` (2026-09-02)

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

### PR #3 review round

Nine findings from `contrasam`, all legitimate; each verified against the branch before fixing.

| Finding | Fix |
|---|---|
| Query answered before historical signals replayed, returning state contradicting durable history | Query subscription now opens *after* `replayHistoricalSignals()` |
| Slow query handler holding `signalLock` stalls the shared single-threaded event loop for every workflow on the worker | Signal delivery moved to a per-workflow single-thread virtual executor, so the loop never blocks on `signalLock`. Blast radius now matches what the javadoc promises |
| `ObservabilityTest.pendingWorkflowsGaugeIsZeroAfterCompletion` became a race under gumbo 0.6's per-subscription delivery threads — failed in this PR's own CI | Assertion polls for the gauge instead of reading it synchronously |
| Both sides re-scanned the ever-growing query tag from `BEGINNING`; worker restart re-answered every historical request | Client subscribes from the tag's current tip; worker subscribes from tip and does one startup scan answering only unanswered requests newer than `STALE_REQUEST_CUTOFF` |
| `answeredQueryIds` grew without bound | Replaced with a seqnum high-water mark — O(1) memory |
| One undeserializable entry logged as an anonymous listener failure | Both listeners and the startup scan catch, log with seqnum, and skip |
| Inherited `@QueryMethod` failed as `UnknownQuery` (`getDeclaredMethods()`) | Resolution and validation use `getMethods()`, with override-aware duplicate detection so a re-declared query is not mistaken for a name clash. Same fix applies to signals |
| `queryTimeout` did not cover the blocking request append | One deadline now covers append and response wait |
| Wire-name resolution duplicated in five places | Single `WireNames` helper |

Three new regression tests, each verified to fail without its fix: inherited query resolution,
the event-loop stall, and no duplicate responses after a worker restart.

Two notes on the review itself:

- The poison-entry finding overstated the consequence. gumbo 0.6.0's `SubscriptionImpl.deliver`
  catches `Throwable` per entry and keeps the subscription alive precisely so "a broken listener
  must not become a broken subscription", so one bad entry cannot stop query answering. The
  guard is still right — it names the seqnum and keeps the startup scan safe.
- The stall test took three attempts to become a real regression test. The first two passed
  without the fix: activity result delivery is the path that actually needs the event loop (a
  workflow that merely returns a value never touches it), and `supplyAsync` returns immediately,
  so the signal had to be gated on the handler actually holding the lock.

### Known limitations to address later

- The `workflow-queries:{workflowId}` tag still has no retention. Client and worker now both
  read from the tip, so neither re-scans it during normal operation, but the tag itself grows
  and the worker's one-time startup scan is O(tag). Trimming belongs with the Phase 13 history
  retention work.
- Each `WorkflowRunner` opens a second subscription plus a signal thread. Worth revisiting as
  one query subscription per worker keyed by task queue if either becomes a scaling concern.

## Output

Merged in PR #3. Implementation:

- `0644ccf` feat(09-01): add QueryMessage sealed hierarchy and QuerySerializer
- `8f1ca9b` feat(09-01): add WorkflowQueryException and WorkflowOptions.queryTimeout
- `44ab372` feat(09-01): validate query methods at workflow registration
- `3c57e26` feat(09-01): handle query requests in WorkflowRunner and WorkflowContext
- `697696f` feat(09-01): wire query round trip into WorkflowStub and SignalOnlyStub
- `4c46fc4` test(09-01): end-to-end query tests
- `8fd0e2d` docs(09-01): document query methods in README

Review fixes:

- `9c516de` fix(09-01): await the pending-workflows gauge instead of reading it synchronously
- `259dcdb` fix(09-01): resolve inherited signal and query methods, via one shared name helper
- `c2f47bc` fix(09-01): harden worker-side query handling
- `71e8888` fix(09-01): bound the whole query round trip and stop rescanning the query tag
- `a202c19` test(09-01): regression tests for the PR #3 review findings
