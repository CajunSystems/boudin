# State

## Current Position

- **Milestone:** 2 — Operable Boudin (In Progress)
- **Phase:** 10 — Async Start & Workflow Handles (built, uncommitted)
- **Plan:** 10-01 — Complete; 09-01 and 08.1-01 merged in PR #3 (`7c4f804`)
- **Next:** Phase 11 — Cancellation & Termination
- **Status:** Phases 8.1 and 9 merged to `main`, CI green. Phase 10 built on
  `feat/phase-10-workflow-handles`: `mvn clean verify` passes 54/54.

## Key Decisions

- **Async start is a lambda, not per-arity functional interfaces** (Phase 10).
  `client.start(() -> stub.process(order))` puts the stub into async mode via a thread-local,
  so `Supplier`/`Runnable` cover every workflow signature where Temporal's `start(stub::m, args)`
  spelling needs `Func0..FuncN` and `Proc0..ProcN`. Arguments stay compiler-checked.
- **`describe()` is log-derived, queries are worker-derived** (Phase 10). A handle answers for
  completed and unhosted executions; a `@QueryMethod` reports live in-memory state and only
  while a worker hosts the workflow. The two are not alternatives.

- Hybrid NIO event loop (infrastructure) + virtual threads (user code)
- Gumbo KV store for worker checkpoints — eliminates full log scan on startup
- Reliability first — fix correctness before adding features
- No Temporal API compatibility — own API designed for the log model
- **Gumbo 0.6.0 as dependency** (Phase 8.1, up from 0.2.0). 0.2.0's `subscribe` lost any entry
  appended while a subscription was still delivering its backlog, which hung `mvn verify`.
  0.3.0 rebuilt delivery; 0.6.0 also fixes multi-tag stream numbering, which silently affected
  Boudin's dual-tag history+task-queue append. No Boudin code changes were required.
- **Queries are control plane, not history** — `QueryMessage` is a separate sealed hierarchy on
  a separate `workflow-queries:{workflowId}` tag. Putting query traffic in `workflow-history`
  would pollute replay and grow history without bound.
- **Milestone 2 theme is the control plane** — observe and steer running executions from
  outside. Determinism/versioning deferred to Milestone 3 as a coherent "safe to deploy twice"
  unit.

## Active Issues

- `ChildWorkflowStub.startChildWatcher` performs a blocking `sharedLog.append().join()` inside a
  subscription listener. Harmless-ish since gumbo 0.3.0 serialises each subscription's
  deliveries on its own thread, but it delays subsequent child-history deliveries and it is the
  pattern that made the old lost-notification window wide. Move the append off the listener.
- Boudin cursors task queues by `seqnum`; gumbo 0.6.0 now provides correct per-tag stream
  versions. Phase 12 should choose deliberately which one task claiming reads on.
- `ActivityDispatcher` KV checkpoint key is per-tag, not per-worker — two workers on one task
  queue clobber each other's checkpoint. Scheduled for Phase 12.
- No task claiming — two workers on one task queue both execute every activity, despite the
  README promising horizontal scale-out. Scheduled for Phase 12.
- README documents `client.newSignalOnlyStub(...)`, which does not exist on `WorkflowClient`
  (the real method is `newWorkflowStub(iface, workflowId, taskQueue)`). Fixed in Phase 9.

## Notes

---
*Initialized: 2026-04-25*
*Milestone 2 planned: 2026-09-02*
