# Session Summary: SSE Retry, Reconnection, and Backfill Recovery Design

**Date:** 2026-06-30
**Duration / Scope:** Design-only session; reviewed current reconnect logic and produced a validated architecture for resilient SSE ingestion with backfill recovery.

---

## High-Level Summary (for quick orientation)

This session designed the retry, reconnection, and data recovery strategy for `event-stream/src/producer_prod.py`. No code was written; this session ends at the architecture design stage. The next session should write the spec doc and implementation plan.

**The core decision:** adopt a two-regime recovery model:
- **Short/medium gaps (< ~7 days):** SSE `Last-Event-ID` resumption — live stream replays gap naturally, no separate stream needed.
- **Long gaps / deliberate fresh restart:** two-stream model — live producer restarts from current position (`restart_event_id`), a separate backfill script fills the gap window defined by `last_event_id → restart_event_id`. A gap list handles cascading failures.

Both streams write to Kafka (separate topics). PostgreSQL receives data from a Kafka consumer with upserts on `meta.id`.

---

## Key Insights

- The existing retry loop in `producer_prod.py` is structurally sound (exponential backoff, bounded timeouts). The critical missing piece is that it never persists or passes `Last-Event-ID` — every reconnect starts from live, silently dropping the gap.
- The two-stream (live + backfill) approach is the correct pattern for large outages: it avoids the catch-up latency problem where a resumed stream lags hours behind live while Wikimedia replays high-volume historical events.
- `restart_event_id` is only meaningful when the operator deliberately chooses to restart from a fresh position. If using pure SSE resumption, `restart_event_id` is redundant — the gap doesn't exist.
- The checkpoint must be written in the Kafka **delivery callback** (after confirmed delivery), not when the event is read from SSE. Writing on read means the checkpoint can advance past events Kafka never received if a crash occurs between read and delivery.
- Two separate checkpoint files are cleaner than one shared file: `resume_state.json` (written by live producer) and `backfill_queue.json` (written by backfill script) avoid write contention.
- Wikimedia EventStreams supports `since` query parameter (timestamp-based) in addition to `Last-Event-ID` (ID-based). The backfill script may prefer `since` since old IDs may expire after ~7 days.

## Knowledge Gaps

- Whether `requests_sse` library exposes `event.id` in a way that can be passed as `Last-Event-ID` on reconnect needs verification before implementation.
- Exact Wikimedia EventStreams history window (assumed ~7 days) should be verified from official documentation before defining the "short gap" boundary.
- Whether Wikimedia's `since` parameter is more reliable than `Last-Event-ID` for the backfill case (ID expiry risk) needs confirmation.
- Startup coordination between live producer and backfill script: the backfill script needs `pending_gaps[0].end_id` to be set before it starts. The coordination mechanism (polling checkpoint file, Docker health signal, etc.) is unresolved.

## Problems Identified

- `event-stream/src/producer_prod.py`: no `Last-Event-ID` is persisted or passed on reconnect — every reconnect starts from live, silently dropping gap events.
- `event-stream/src/producer_prod.py`: no checkpoint file written after delivery confirmation — producer has no durable resume position across container restarts.
- Concurrent writes to a shared checkpoint file (live producer + backfill script) would cause contention if implemented as a single file.

## Solutions Implemented

_Nothing to note._ (Design-only session; no code changes made.)

## Deferred Work

- [ ] Verify that `requests_sse` exposes `event.id` and supports passing `Last-Event-ID` on reconnect.
- [ ] Verify Wikimedia's EventStreams history retention window.
- [ ] Write the formal spec doc to `docs/superpowers/specs/2026-06-30-sse-retry-recovery-backfill-design.md`.
- [ ] Invoke `writing-plans` skill to produce the implementation plan from the spec.
- [ ] Implement `resume_state.json` checkpoint write in the Kafka delivery callback in `producer_prod.py`.
- [ ] Implement live producer startup logic: read `resume_state.json`, record `restart_event_id` as first confirmed delivery of new session, append gap entry to `backfill_queue.json`.
- [ ] Build `backfill.py` (separate script/container): reads `backfill_queue.json`, opens SSE stream from `start_id`/`start_dt`, produces to `wikimedia.recentchange.backfill` topic, terminates when `event.meta.dt >= end_dt`, removes completed gap entry.
- [ ] Decide and document startup coordination mechanism between live producer and backfill script.
- [ ] Add `wikimedia.recentchange.backfill` topic to `docker-compose.yml` or Kafka topic setup.

---

## Architecture Reference (for cold-start orientation)

```
resume_state.json (Docker volume)
  └── last_event_id, last_event_dt (updated on each Kafka delivery confirm)

backfill_queue.json (Docker volume)
  └── pending_gaps: [{start_id, start_dt, end_id, end_dt}, ...]
      Written by live producer on restart; consumed and cleared by backfill script.

Kafka topics:
  wikimedia.recentchange.raw       ← live producer (producer_prod.py)
  wikimedia.recentchange.backfill  ← backfill script (backfill.py)

PostgreSQL (via Kafka consumer, future phase):
  recentchange_raw — upsert on meta_id, receives from both topics
```

---

_Captured by /save-session_
