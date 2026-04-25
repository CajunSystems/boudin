# State

## Current Position

- **Milestone:** 1 — Production-Ready Boudin
- **Phase:** 3 — NIO Event Loop Infrastructure
- **Plan:** 03-02 — Route WorkflowRunner history events through event loop
- **Status:** Planned, ready to execute

## Key Decisions

- Hybrid NIO event loop (infrastructure) + virtual threads (user code)
- Gumbo KV store for worker checkpoints — eliminates full log scan on startup
- Reliability first — fix correctness before adding features
- No Temporal API compatibility — own API designed for the log model
- Gumbo 0.2.0 as dependency (updated from main-SNAPSHOT)

## Active Issues

None

## Notes

---
*Initialized: 2026-04-25*
