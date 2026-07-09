# Session Summary: SSE Retry/Resumption Design Brainstorm (In Progress)

**Date:** 2026-07-04
**Duration / Scope:** Brainstorming session (superpowers:brainstorming skill), still mid-flow — no design doc written yet, no code changed.

---

## Key Insights

- `requests_sse` (already in `event-stream/requirements.txt`) natively supports resumption: `EventSource.__init__` accepts `latest_event_id` (sent as the `Last-Event-Id` header), and every `MessageEvent` carries `.last_event_id`. This resolves a knowledge gap left open by the 2026-06-30 design session (`claude-sessions/2026-06-30-sse-retry-recovery-backfill-design.md`).
- Scope for this design was narrowed to **retry/resumption only** — the two-regime backfill system (`backfill.py`, `backfill_queue.json`, separate Kafka topic) from the 2026-06-30 session stays out of scope and becomes its own future spec. (User's original AskUserQuestion scope prompt timed out with no response; Claude proceeded on the recommended option — retry-only — and the user's subsequent messages did not contradict this, so it stands as the working scope.)
- Rejected sub-approaches: in-memory-only resumption (loses checkpoint on crash, violates CLAUDE.md's "persistent Last-Event-ID" principle); checkpoint-on-SSE-read instead of on-delivery-confirm (already flagged wrong in the 2026-06-30 session — reconfirmed here with a concrete failure timeline, see below).
- Reviewing the current `event-stream/src/producer_prod.py` against "enterprise production pipeline" surfaced several gaps beyond simple checkpointing:
  - `FAILED_RETRY` (default 5) is a **lifetime** counter that never resets — after 5 disconnects ever (even months apart), the process exits permanently. Needs to become a consecutive-failure counter that resets after a sustained healthy period.
  - No jitter in the backoff (`wait = min(wait * 2, MAX_RETRY_WAIT)`), despite CLAUDE.md explicitly requiring "exponential backoff + jitter" — a real Wikimedia-wide outage would cause a thundering herd of synchronized reconnects.
  - `except Exception:` in `main()` is too broad, violating the project's own "catch specific exceptions, never bare except" convention. Needs to distinguish retryable transient errors (network/socket) from unexpected ones (which should crash loud with traceback, not retry forever).
  - No heartbeat/dead-man's-switch file exists yet, despite being named as a required pattern in CLAUDE.md.
  - Checkpoint writes must be atomic (temp file + `os.replace()`) from the start — a partial write on crash would corrupt the checkpoint, which is worse than having none.
- Resolved a real design question about "multiple last_event_id": there is only ever **one** persisted checkpoint. It only advances on Kafka delivery-confirmation (never regresses), so no matter how many consecutive failures occur, there's exactly one authoritative on-disk value at any instant — never a list or queue. The "multiple IDs" concept only applies to the (out-of-scope) backfill gap queue, which is a structurally different mechanism — a ledger of abandoned gap ranges consumed by a separate `backfill.py` process — not a substitute for the single checkpoint.
- Corrected an error in Claude's own earlier proposal: initially suggested using an in-memory "last read event ID" for in-process reconnects (not just process-restart resumption). This is wrong — the library's in-memory tracker updates on every SSE **read**, which can run ahead of what Kafka has actually confirmed delivered (messages can be read from SSE, queued in the producer, but not yet delivery-confirmed when a connection drops). Corrected design: every reconnect, in-process or cross-restart, must re-read the on-disk (delivery-confirmed) checkpoint fresh before opening a new `EventSource` — never reuse an in-memory value across a failure boundary.
- Why replaying already-confirmed messages after resumption is safe: CLAUDE.md's dedup principle (`meta.id` + idempotent Kafka producer + PostgreSQL upsert) absorbs any duplicate replay for free, so resuming from a slightly-stale but *confirmed* checkpoint is always safer than resuming from a fresher but *unconfirmed* one.

## Knowledge Gaps

- Wikimedia's actual SSE server behavior when given an expired/very old `Last-Event-Id` is still unverified — does it silently ignore the header and resume from live, or error? Assumed ~7 day replay window per the 2026-06-30 session, not confirmed against live docs or API testing. This directly affects the proposed `MAX_RESUMABLE_GAP_SECONDS` warning threshold.
- The exact duration for "sustained healthy streaming" that resets the consecutive-failure counter has not been numerically decided yet.

## Problems Identified

- `event-stream/src/producer_prod.py:20` (`FAILED_RETRY`) — lifetime retry cap, never resets, kills long-running process permanently after 5 total disconnects.
- `event-stream/src/producer_prod.py:130-137` (`handle_retry`) — backoff has no jitter.
- `event-stream/src/producer_prod.py:142,151` — `except Exception:` too broad; doesn't distinguish retryable vs fatal errors.
- No heartbeat file / dead-man's-switch implementation exists anywhere in `event-stream/src/`.
- No checkpoint file exists yet (expected, since resumption isn't implemented) — but must be designed atomic from the start.

## Solutions Implemented

_Nothing to note._ (Design-only session; no code changes made. Task list #1-8 created in-session to track the brainstorming checklist — #1 explore-context and #2 clarifying-questions marked completed, #3 propose-approaches was in progress when the session was saved.)

## Deferred Work

- [ ] Finish the full design presentation (architecture / data flow / error handling / testing sections) per the brainstorming skill checklist.
- [ ] Write the design doc to `docs/superpowers/specs/2026-07-04-sse-retry-logic-design.md` once the design is approved.
- [ ] Run spec self-review (placeholder scan, internal consistency, scope check, ambiguity check).
- [ ] Get user review of the written spec before moving to implementation planning.
- [ ] Invoke the `writing-plans` skill to produce the implementation plan.
- [ ] Verify Wikimedia's actual behavior for an expired/stale `Last-Event-Id` (carried over from the 2026-06-30 session, now more urgent since it affects the gap-warning threshold design).
- [ ] Decide the numeric "consecutive failure counter reset" window.
- [ ] Decide the numeric `MAX_RESUMABLE_GAP_SECONDS` default (currently a placeholder idea, not yet fixed).

---

_Captured by /save-session_
