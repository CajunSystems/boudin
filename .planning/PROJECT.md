# Boudin Workflow Framework

## What This Is

A production-ready durable workflow orchestration framework for Java, modeled after Temporal but built natively on the Gumbo shared append-only log. Workflows are deterministic, fault-tolerant, and persistent by design — the log is the only store.

## Core Value

Developers write normal sequential Java code. Boudin makes it durable, recoverable, and observable without any external database, message broker, or scheduler.

## Architecture Principles

- **Log-native**: Gumbo is the only store. No Redis, no Postgres, no Kafka.
- **Hybrid execution**: NIO event loop drives infrastructure (dispatch, timers, subscriptions); virtual threads run user workflow/activity code.
- **KV checkpoints**: Use Gumbo 0.2.0 per-tag KV store for durable checkpoints (replaces unbounded startup log scans).
- **Own API**: Not a Temporal drop-in. API is designed to fit the log model cleanly.
- **Java 21 only**: Virtual threads and sealed types are hard requirements.

## Requirements

### Validated

- ✓ Workflow execution with deterministic replay — existing
- ✓ Activity execution and result delivery — existing
- ✓ Signal handling (@SignalMethod, Workflow.await()) — existing
- ✓ Workflow/activity proxy stubs via dynamic proxies — existing
- ✓ Kryo serialization for arbitrary POJOs — existing
- ✓ Crash recovery via log replay — existing (but doesn't scale)
- ✓ Multi-tag atomic log appends — existing

### Active

- [ ] NIO event loop infrastructure — replace subscription-callback dispatchers with a selector-based event loop; timer wheel lives here
- [ ] Workflow.sleep() and timers — Workflow.sleep(Duration), Workflow.await(timeout, condition) backed by timer wheel
- [ ] Activity retries — enforce maxAttempts with configurable backoff; currently defined but ignored
- [ ] Activity timeouts — enforce startToCloseTimeout; currently defined but ignored
- [ ] Child workflows — start a workflow from within a workflow; fan-out, saga patterns
- [ ] Exactly-once activity semantics — fix at-least-once gap; activity must not re-execute after result is written
- [ ] KV-based crash recovery — use Gumbo KV checkpoints to avoid full log scan on worker startup
- [x] Query methods — read workflow state synchronously without waiting for completion (Phase 9)
- [ ] Observability — structured metrics (workflow/activity latency, failure rates, queue depth), MDC tracing, health endpoints
- [ ] Workflow timeout enforcement — enforce WorkflowOptions.workflowRunTimeout
- [ ] Idempotent workflow start — built-in dedup on workflowId so double-start is safe

### Out of Scope

- Temporal API compatibility — own API designed for the log model; not a drop-in replacement
- Web UI / workflow visibility dashboard — separate project
- Multi-language SDKs — Java only for this evolution
- External persistence — no DB, no Redis, Gumbo is the store

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Hybrid NIO + virtual threads | NIO event loop for infrastructure (dispatch, timer wheel) preserves developer experience of sequential workflow code on virtual threads | Architecture target |
| Gumbo KV for checkpoints | Eliminates unbounded startup log scan; workers resume from last checkpoint instead of replaying full history | Active requirement |
| No Temporal API compat | Clean API that fits the log model; avoids impedance mismatch and unnecessary complexity | Hard constraint |
| Java 21 only | Virtual threads and sealed types are first-class; backport adds no value | Hard constraint |
| Gumbo 0.2.0 | KV store, TypedLogView, readAfter() positional reads, subscribeTail() all needed for correct implementation | pom.xml updated |
| Reliability first | Exactly-once semantics and correct crash recovery before new features | North star |

## Open Issues (from codebase analysis)

- Signal delivery not idempotent — duplicate delivery fires signal handler twice
- Pending activities map never pruned — memory accumulates in long-running workflows
- No validation at workflow/activity registration — errors surface at runtime
- Thread.sleep(200) in tests for async sync — fragile, should use Awaitility

---
*Last updated: 2026-04-25 after initialization*
