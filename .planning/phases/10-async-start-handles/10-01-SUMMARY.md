# Phase 10, Plan 1: Async Start & Workflow Handles — Summary

## Status: Complete, PR #4 review addressed (uncommitted)

## What Was Built

A workflow can now be started without holding the caller for its lifetime, and its result
collected later from any process holding the workflow ID.

`WorkflowClient.start(() -> stub.process(order))` returns a `WorkflowHandle<T>` as soon as the
workflow is durably recorded. `WorkflowClient.getHandle(workflowId)` rebuilds a handle anywhere.
The handle offers `getResult()`, `getResult(Duration)`, `getResultAsync()` and `describe()`, the
last reading `WorkflowExecutionDescription` — status, type, task queue, start and close times,
history length, and failure detail — straight from the log. `listWorkflows(taskQueue)` exposes
the dispatcher's active-workflow set.

## Tasks Completed

| Task | Description |
|------|-------------|
| 1 | `WorkflowStatus`, `WorkflowExecutionDescription` (+ nested `Failure`) |
| 2 | `WorkflowHandle` and `WorkflowTimeoutException` |
| 3 | Async start mode in `WorkflowStub`; `appendStart` shared with blocking start |
| 4 | `WorkflowClient.start`/`getHandle`/`listWorkflows`; `KV_ACTIVE_WORKFLOWS` made public |
| 5 | `WorkflowHandleTest` — 14 tests, plus 5 from the review round |
| 6 | README section, TOC, package structure, and `WorkflowClient` javadoc |

## Key Design Decisions

- **A lambda, not per-arity functional interfaces.** `client.start(() -> stub.process(order))`
  rather than Temporal's `start(stub::process, order)`. A thread-local set by `start` puts the
  stub into async mode, so the workflow method call appends `WorkflowStarted`, records the ID,
  and returns. Arguments are supplied by ordinary Java at the call site, so two interfaces
  (`Supplier`, `Runnable`) cover every workflow signature where the Temporal spelling needs
  `Func0..FuncN` plus `Proc0..ProcN`. The deviation from the roadmap's `start(stub, args)` is
  deliberate: same idea, arguments type-checked by the compiler instead of passed as `Object...`.
- **Async start returns a type-appropriate default, not null.** A workflow method declared `int`
  would have a `null` unboxed at the lambda's return, throwing `NullPointerException` nowhere
  near the cause. `defaultValueFor` returns `0`, `false`, `'\0'` and so on. There is a test for
  exactly this.
- **One append path.** `appendStart` is shared by blocking and async start, so the idempotency KV
  check — the property that makes a retrying caller safe — cannot drift between them.
- **`getResult` takes the tip, subscribes past it, then reads up to it.** Reading first would drop
  a terminal event landing between read and subscribe; subscribing from `BEGINNING` would
  re-deserialize the entire history on every call, the same cost Phase 9's review flagged on the
  query tag. This ordering has neither problem and resolves an already-finished workflow without
  waiting.
- **`describe()` is log-derived, not worker-derived.** It answers for completed and unhosted
  executions, which is precisely what a `@QueryMethod` cannot do. Status is a function of the
  terminal event, whatever any worker currently believes.
- **`listWorkflows` is documented as what it is.** It reads the dispatcher's per-task-queue active
  set: one queue only, empty until a worker has run there, and a workflow whose worker died
  without processing its terminal event lingers until recovery. It is not a query index, and the
  javadoc and README say so rather than letting callers assume otherwise.
- **`WorkflowTimeoutException` is about the wait, not the workflow.** Its message says so
  explicitly, and a test asserts the same handle still resolves afterwards. Distinct from
  `WorkflowOptions.workflowRunTimeout`, which is a limit on the execution and is Phase 11's.

## Test Results

`mvn clean verify` — **59/59 pass**, including the 19 new tests.

Coverage of note: start returning while the workflow is still parked; result retrieval for an
already-completed workflow via a freshly-built handle; cross-client reattach by ID alone;
`getResult(Duration)` expiring and the handle still working afterwards; failure propagation;
`describe()` across RUNNING → COMPLETED and on a failure; `listWorkflows` gaining and losing an
entry; a lambda that starts nothing being rejected; the primitive-unboxing trap; the `void`
overload; and a double start on one workflow ID producing a single execution.

## Issues / Deviations

- `WorkflowStub.startAndWait` was restructured so both start paths share `appendStart`. When the
  workflow ID is generated rather than supplied, the history tag is unknown until the append
  returns, so the subscription is opened afterwards from `BEGINNING` — equivalent, because the
  backlog carries any terminal event already written. Blocking-start behaviour is unchanged and
  the existing tests cover it.
- An abandoned `getResultAsync()` future leaves its subscription open until the workflow
  terminates. Cancelling the returned future releases it immediately; the review round made that
  the documented contract rather than claiming abandonment was enough.

## PR #4 review round

Seven findings from `contrasam`, all legitimate; each verified against the branch before fixing.

| Finding | Fix |
|---|---|
| `start()` bound a cross-client stub to the wrong `SharedLog`, so `getResult()` blocked forever on an empty history tag — and the javadoc promised a check that did not exist | `AsyncStart` records the log the workflow was started on; `startInternal` rejects a mismatch. The javadoc now lists what is actually enforced |
| Each `getResult(timeout)` that expired leaked a live subscription, despite the javadoc claiming otherwise — and the exception's own message invites retrying | The timeout and interrupt paths cancel the future, and `getResultAsync` closes the subscription when the *returned* future settles, cancellation included. The javadoc now says dropping the reference is not the same as cancelling |
| The supplied-workflowId subscription leaked when `appendStart` threw, since the append ran before the `try` | `appendStart` moved inside the `try`, with a null guard for the generated-ID path where the subscription does not exist yet |
| Two workflow calls in one `start()` lambda started both but returned a handle to only the second; a nested `start()` cleared the outer thread-local and dropped the outer lambda into blocking start | Both now throw `IllegalStateException` — the second before it durably starts anything, and nesting at `beginAsyncStart` |
| `listWorkflows` leaked a raw `KryoException` from a public API on an unreadable KV value | Degrades to an empty list with a warning, matching `WorkflowDispatcher.loadActiveWorkflows` on the same key |
| `getResultAsync` blocked on a full history read before returning the future, defeating the async variant | The catch-up read is chained rather than joined; the subscription is already in place, so nothing can slip between the two |
| `completeFrom` duplicated between `WorkflowHandle` and `WorkflowStub` | Extracted to `TerminalEvents` — the same move PR #3 made with `WireNames`. Adding a terminal event now changes one place, so blocking stubs and handles cannot disagree about when a workflow has finished |

Five new tests. Four are genuine regression tests, verified to fail with their fix reverted:
cross-log start, two starts in one lambda, nested start, and the unreadable KV value.

**One is not.** `repeatedTimedWaitsOnAParkedWorkflowStillResolve` passes with or without the
subscription-cancel fix, because an open subscription is not observable through the public API.
It is a smoke test that repeated timed waits still resolve, not proof the leak is gone — the
proof is that `getResultAsync` now closes on the returned future settling, cancellation included.

## Follow-Ups

- `WorkflowHandle.describe()` and `getResultAsync()` both call `readAll()` on the history tag,
  which is O(history). gumbo 0.6.0 has `readPrevBefore`, so a terminal-event lookup could read
  backwards from the tip instead. Worth doing if handles get used in a polling loop.
- `getHandle(workflowId, Class<T>)` does not verify the type against the payload — same
  unchecked contract as a blocking stub. A real check would need the declared type recorded at
  start.
- Phase 11 adds `cancel`/`terminate` to the handle, and `WorkflowStatus` gains `CANCELLED` and
  `TERMINATED`; Phase 13 adds `CONTINUED_AS_NEW` and run-chain resolution.

## Output

Per-task commits:
- `feat(10-01): add WorkflowStatus and WorkflowExecutionDescription`
- `feat(10-01): add WorkflowHandle and WorkflowTimeoutException`
- `feat(10-01): add async start mode to WorkflowStub`
- `feat(10-01): add WorkflowClient.start, getHandle and listWorkflows`
- `test(10-01): workflow handle and async start tests`
- `docs(10-01): document async start and handles`
