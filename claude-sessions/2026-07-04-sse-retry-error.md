# Session Summary: SSE Retry Log Verification — Bugs Found Against Real Disconnect Tests

**Date:** 2026-07-04
**Duration / Scope:** Verified `event-stream/src/producer_prod.py` behavior against two real disconnect-test logs (`timeout_1.log`, `timeout_2.log`); no code changed, four concrete defects found.

---

## Key Insights

- `FAILED_RETRY=5` being a **lifetime** (not consecutive) counter is not just a theoretical bug — `timeout_1.log` shows it happen for real: exactly 5 `NameResolutionError` failures at 19:23:28, 19:23:52, 19:24:18, 19:24:45, 19:25:12, then the process exits with **zero log output** (no "giving up," no non-zero exit signal). `retry < FAILED_RETRY` (`producer_prod.py:156`) goes false and `main()` just returns.
- The configured backoff (`RETRY_WAIT`/`MAX_RETRY_WAIT` → sleeps of 2, 4, 5, 5, 5s) is not what drove the ~24-27s gaps between retries in `timeout_1.log`. Real reconnect cadence during an actual outage is dominated by OS-level `getaddrinfo()`/connection-attempt latency, not the app's own sleep() calls — worth remembering when reasoning about thundering-herd risk from missing jitter.
- The "missing" `BrokenPipeError`/librdkafka `TERMINATE` block the user noticed absent from `timeout_2.log` is a shell artifact, not a code bug: `python producer_prod.py | tee -a timeout_2.log` puts both processes in the same foreground process group. Ctrl+C sends `SIGINT` to both; `tee` has no custom handler and dies instantly, while `producer_prod.py`'s `handle_shutdown` (`producer_prod.py:34-42`) deliberately delays exit to flush Kafka. Both missing messages (librdkafka's native warning, Python's own "Exception ignored ... BrokenPipeError" at interpreter finalization) are written to **stderr**, which was never captured because the command lacked `2>&1`.
- That shell artifact exposes a real, non-shell-specific robustness gap: the `finally` block in `kafka_produce` (`producer_prod.py:133-139`) does `print("Flushing messages...", flush=True)` **before** calling `producer.flush(...)` on the next line. If stdout's pipe is already broken at that point, the print raises `BrokenPipeError` and `producer.flush()` never executes — meaning the graceful Kafka flush-on-shutdown can be silently skipped, which is consistent with librdkafka later reporting undelivered messages still in queue at Producer teardown.

## Knowledge Gaps

- Whether Docker's log driver (the actual production log-capture path, vs. `| tee` in a manual test) has the same premature-pipe-close risk during graceful shutdown — unconfirmed, worth checking before relying on `docker logs` capturing full shutdown sequences.

## Problems Identified

- `producer_prod.py:22,156` (`FAILED_RETRY`) — lifetime retry cap confirmed in practice via `timeout_1.log`: silent process death after exactly 5 disconnects, no crash-loud/escalation signal at all.
- `producer_prod.py:135-136` (`kafka_produce` `finally` block) — `print(...)` runs before `producer.flush(...)`; an unguarded `BrokenPipeError` (or any stdout write failure) on that print skips the flush call entirely, risking silent message loss on shutdown.
- `producer_prod.py:108-121` (JSON parse block) — `except ValueError: pass` is checked before `except Exception as error:`. Since `json.JSONDecodeError` is a `ValueError` subclass, malformed events are always silently swallowed by the `pass` branch and fall through to `value = json.dumps(change)` using the **stale `change` from the previous iteration** — silently republishing the previous message to Kafka instead of logging/counting the failure. The `point_retry`/`hard_retry_limit` machinery never triggers for its intended purpose. Not exercised in either test log (no malformed JSON occurred in this run).
- `producer_prod.py:90` — `if hard_retry_limit == 5: sys.exit` is missing call parentheses (`sys.exit()`); as written it references the function object and does nothing. Dead code, not exercised in either log.

## Solutions Implemented

_Nothing to note._ (Verification-only session; no code changed.)

## Deferred Work

- [ ] Make `FAILED_RETRY` a consecutive-failure counter that resets after sustained healthy streaming, and add a crash-loud escalation log when the cap is exhausted (ties into the clustering/escalation design in [2026-07-04-sse-retry-resumption-design-brainstorm.md](2026-07-04-sse-retry-resumption-design-brainstorm.md)).
- [ ] Add jitter to the reconnect backoff (still missing).
- [ ] Reorder or guard the `finally` block in `kafka_produce` so `producer.flush()` cannot be skipped by an unrelated `print()` failure — e.g. call `flush()` before the "Flushing messages..." print, or wrap the prints in try/except.
- [ ] Fix the `except ValueError: pass` / `except Exception` ordering bug in the JSON parse block (`producer_prod.py:108-121`) so malformed JSON is logged/counted instead of silently republishing the previous message.
- [ ] Fix `sys.exit` → `sys.exit()` at `producer_prod.py:90`.
- [ ] Verify whether Docker's log driver has the same premature-pipe-close risk as `| tee` for capturing graceful-shutdown output.

---

_Captured by /save-session_
