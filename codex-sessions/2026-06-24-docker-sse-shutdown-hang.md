# Docker SSE Shutdown Hang

Date: 2026-06-24

## Summary
- Investigated why the `python-sse` Docker service still appeared to hang even after `event-stream/src/producer_prod.py` had been hardened for SSE reconnects and bounded Kafka flushes.
- Found that the Docker image was still configured to run `src/producer.py`, not `src/producer_prod.py`, so rebuilding copied the new file but the running container continued executing the old producer path.
- Also identified a Docker shutdown-specific blocking path: Docker sends `SIGTERM`, the Python handler set `shutdown=True`, but the process could remain blocked inside the `requests_sse.EventSource` iterator and never return to the loop body to observe the shutdown flag.
- Implemented a Docker-focused fix: run `producer_prod.py` in the image, track and close the active SSE stream on shutdown, break out of the outer retry loop when shutdown is in progress, and give Docker a longer stop grace period.

## Key Points
- User evidence showed local `producer_prod.py` retry behavior working for a bad SSE endpoint:
  - `requests_sse.client.InvalidStatusCodeError`
  - HTTP `400`
  - URL shown as `https://stream.wikimedia.org/v2/stream/recentchang`, missing the final `e`.
- The HTTP `400` was caused by a bad/typoed stream URL in that local test and was separate from the Docker hang.
- Docker logs showed the container received shutdown:
  - `Shutdown requested.`
  - Then Docker stopped the container with exit code `137`.
- Exit code `137` means the process was killed with `SIGKILL`, usually after it did not exit during Docker's graceful stop window.
- `docker compose build python-sse` only builds the image. It does not replace an already-running container by itself. The service must be recreated with a command such as:
  - `docker compose up -d --build --force-recreate python-sse`

## Issues Encountered
- Dockerfile entrypoint mismatch:
  - Before the fix, `event-stream/Dockerfile` used `CMD ["python", "src/producer.py"]`.
  - That meant Docker was still running the older producer implementation with the unbounded flush and no `producer_prod.py` retry/shutdown behavior.
- Blocking SSE read on shutdown:
  - The signal handler changed `shutdown` to `True`, but the code could still be waiting inside `for event in stream:`.
  - If the iterator was blocked in an HTTP/SSE socket read, execution might not return promptly to this check:
    - `if shutdown: break`
- Docker signal behavior:
  - Docker sends `SIGTERM` to PID 1 on graceful stop.
  - If the process does not exit before the grace period, Docker sends `SIGKILL`, producing exit code `137`.
- Verification limits:
  - The current Codex environment could inspect and edit files but could not run Docker validation.
  - `python3 -m py_compile` attempted to write `__pycache__` and failed under a read-only sandbox, so an in-memory `compile(...)` check was used instead.

## Debugging / Investigation Steps
- Inspected `event-stream/Dockerfile` and found:
  - `CMD ["python", "src/producer.py"]`
- Inspected `docker-compose.yml` and the `python-sse` service config.
- Inspected `event-stream/src/producer.py` and confirmed it still used the older top-level producer flow with:
  - `EventSource(...)` without the new bounded retry settings.
  - `producer.flush()` with no timeout.
  - `except (KeyboardInterrupt, RuntimeError, TypeError): pass`.
- Inspected `event-stream/src/producer_prod.py` and confirmed it had the earlier retry hardening:
  - bounded SSE timeout settings.
  - bounded Kafka flush.
  - outer retry handling.
- Ran an in-memory syntax validation after edits:
  - `python3 - <<'PY' ... compile(source, 'event-stream/src/producer_prod.py', 'exec') ... PY`
  - Result: `syntax ok`.

## Changes Made
- Edited `event-stream/Dockerfile`:
  - Changed the container command to run `src/producer_prod.py`.
  - Current command:
    - `CMD ["python", "src/producer_prod.py"]`
- Edited `docker-compose.yml`:
  - Added `stop_grace_period: 30s` to the `python-sse` service.
- Edited `event-stream/src/producer_prod.py`:
  - Added a module-level `current_stream = None`.
  - Updated `handle_shutdown(...)` to declare `global shutdown, current_stream`.
  - On `SIGINT`/`SIGTERM`, the handler now:
    - sets `shutdown = True`.
    - logs `Shutdown requested.`
    - closes `current_stream` when one is active.
  - Updated `kafka_produce(...)` to declare `global shutdown, current_stream`.
  - Stores the active `EventSource` in `current_stream` after opening it.
  - Clears `current_stream` in `finally`.
  - In `main()`, if `kafka_produce(...)` raises while `shutdown` is true, it breaks instead of treating shutdown as another retryable SSE failure.

## New Knowledge / Lessons Learned
- Rebuilding an image is not enough when a container already exists or is running. Recreate the service to use the new image:
  - `docker compose up -d --build --force-recreate python-sse`
- Docker can send `SIGTERM` correctly and the Python signal handler can still appear to hang if the app is blocked inside a socket iterator.
- A shutdown flag alone is not always enough for blocking stream readers. Closing the active stream from the signal handler gives the iterator a reason to unblock.
- Exec-form Docker `CMD` is important for signal delivery:
  - Good: `CMD ["python", "src/producer_prod.py"]`
  - Avoid shell form for this service: `CMD python src/producer_prod.py`
- Exit code `137` is evidence of `SIGKILL`, not clean app shutdown.

## Open Questions / Follow-ups
- Validate with Docker locally:
  - `docker compose up -d --build --force-recreate python-sse`
  - `docker compose logs -f --tail=80 python-sse`
  - `docker compose stop python-sse`
  - Confirm the logs show shutdown, stream close/unblock, bounded flush, and clean exit without code `137`.
- Test direct signal handling:
  - `docker compose kill -s SIGTERM python-sse`
  - `docker compose kill -s SIGINT python-sse`
- Consider whether `producer.py` should be removed, renamed, or kept only as a historical/simple prototype to avoid future Dockerfile confusion.
- Consider adding a small startup log that prints the script name or producer mode so it is immediately obvious which producer file the container is executing.

## Useful References
- `event-stream/Dockerfile`
- `docker-compose.yml`
- `event-stream/src/producer.py`
- `event-stream/src/producer_prod.py`
- Previous related log:
  - `codex-sessions/2026-06-24-producer-network-drop-retry-fix.md`
- Useful commands:
  - `docker compose up -d --build --force-recreate python-sse`
  - `docker compose logs -f --tail=80 python-sse`
  - `docker compose kill -s SIGTERM python-sse`
  - `docker compose kill -s SIGINT python-sse`
