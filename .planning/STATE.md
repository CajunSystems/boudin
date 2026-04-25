# State

## Current Position

- **Milestone:** 1 — Production-Ready Boudin
- **Phase:** 1 — Foundation & Correctness
- **Plan:** 01-01 — Foundation & Correctness
- **Status:** Complete

## Key Decisions

- Hybrid NIO event loop (infrastructure) + virtual threads (user code)
- Gumbo KV store for worker checkpoints — eliminates full log scan on startup
- Reliability first — fix correctness before adding features
- No Temporal API compatibility — own API designed for the log model
- Gumbo 0.2.0 as dependency (updated from main-SNAPSHOT)

## Active Issues

- Unbounded startup log scan (Phase 2)

## Notes

---
*Initialized: 2026-04-25*
