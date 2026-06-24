# Wikimedia SSE 500 Incident

Date: 2026-06-18

## Summary
- The user reported an overnight/long-running pipeline failure from the `python-sse` producer.
- The producer had successfully delivered messages to Kafka topic `wikimedia.recentchange.raw` through partition `0`, offset `703782`, then shut down.
- The traceback showed `requests_sse.client.InvalidStatusCodeError` because Wikimedia EventStreams returned HTTP `500` for `https://stream.wikimedia.org/v2/stream/recentchange`.
- No bug fix was requested or applied in this session. The user explicitly wants to debug and fix it later.

## Key Points
- The failure occurred at the `for event in stream:` loop in `event-stream/src/producer.py`.
- Current exception handling catches `(KeyboardInterrupt, RuntimeError, TypeError)` only, so `InvalidStatusCodeError` is not handled and the process exits after the `finally` block flushes Kafka.
- The Kafka delivery logs immediately before the traceback indicate Kafka delivery was still succeeding; the observed failure is on the Wikimedia SSE fetch side.
- Treat the HTTP `500` as likely retryable/transient upstream behavior unless later evidence shows a local request/configuration issue.

## Issues Encountered
- Reported log excerpt:

```text
2026-06-17 12:19:20.170450 | Loaded 1 message to wikimedia.recentchange.raw partition=0 offset=703781
2026-06-17 12:19:20.170467 | Loaded 1 message to wikimedia.recentchange.raw partition=0 offset=703782
2026-06-17 12:19:20.171467 | Shutdown complete.

Traceback (most recent call last):
  File "/app/event-stream/src/producer.py", line 56, in <module>
    for event in stream:
  File "/usr/local/lib/python3.12/site-packages/requests_sse/client.py", line 248, in __next__
    self.connect(self._max_connect_retry)
  File "/usr/local/lib/python3.12/site-packages/requests_sse/client.py", line 286, in connect
    raise InvalidStatusCodeError(
requests_sse.client.InvalidStatusCodeError: fetch https://stream.wikimedia.org/v2/stream/recentchange failed with wrong response status: 500
```

## Debugging / Investigation Steps
- Inspected repository files with `rg --files`.
- Read `event-stream/src/producer.py`.
- Listed relevant directories with `ls docs kafka event-stream`.
- Checked the producer loop with line numbers using `nl -ba event-stream/src/producer.py | sed -n '40,85p'`.
- Confirmed `producer.py` currently creates an `EventSource`, loops over events, filters non-message events and canary domain events, produces JSON to Kafka, polls delivery callbacks, and flushes in `finally`.
- Confirmed the exception handler starts at line 82 and does not include `requests_sse.client.InvalidStatusCodeError`.
- Checked Git root with `git rev-parse --show-toplevel`.
- Checked current worktree status; unrelated changes already existed in `docker-compose.yml`, `event-stream/src/producer.py`, and untracked `AGENTS.md`.

## Changes Made
- Created this session log under `codex-sessions/`.
- Updated Git-root `AGENTS.md` with a `## Codex Session Logs` link to this file.
- Did not edit `event-stream/src/producer.py`.
- Did not run Docker Compose or any pipeline validation.

## New Knowledge / Lessons Learned
- The current producer gracefully flushes Kafka after the SSE exception because the `finally` block still runs, but it does not restart or reconnect after this `InvalidStatusCodeError`.
- A future fix should likely wrap the `EventSource` connection/iteration in a retry loop and catch the specific requests-sse exception type rather than masking all exceptions.
- For this project, incident notes are also commonly kept under `kafka/`, but `$log-session` logs should live in `codex-sessions/` and be linked from `AGENTS.md` for future Codex discovery.

## Open Questions / Follow-ups
- Decide whether to catch only `InvalidStatusCodeError` or a broader set of requests/network exceptions from `requests_sse` and `requests`.
- Add reconnect logging that distinguishes upstream SSE failures from Kafka delivery failures.
- Add conservative backoff, for example start at a few seconds and cap around one minute.
- Verify shutdown behavior after the retry loop is added so `SIGTERM` still exits promptly and flushes pending Kafka messages.
- Consider documenting the incident separately in `kafka/error-fix.md` if the user wants non-Codex-facing project notes too.

## Useful References
- `event-stream/src/producer.py`
- `kafka/error-fix.md`
- `docker compose logs --tail=120 python-sse`
- `docker compose up -d kafka kafka-ui python-sse`
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list`

## Proposed Plan Captured From Session
- Record the incident in `kafka/error-fix.md` or a new dated note if desired.
- Update `producer.py` later so `InvalidStatusCodeError` does not permanently kill the producer.
- Add retry/backoff around the `EventSource` connection loop while preserving graceful shutdown and Kafka `flush()`.
- Keep existing delivery callback logging, but add clear reconnect/error logs for SSE failures.
- Validate by starting Kafka and the producer, confirming messages still publish, simulating or forcing an SSE failure if practical, and confirming the producer reconnects instead of exiting.
