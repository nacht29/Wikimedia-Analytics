# Project Setup Catch-Up

Date: 2026-06-18

## Summary
- User asked for a detailed catch-up on the current project setup, including Docker services and the Python Wikimedia SSE producer.
- Inspected local repository files rather than running the Docker stack.
- Current implemented stack is narrower than the target architecture in `docs/`: Docker Compose currently defines PostgreSQL, Kafka in KRaft mode, Kafka UI, and a Python SSE producer.
- The broader planned architecture includes Spark, Airflow, MinIO, PostgreSQL marts, and Power BI, but those services are not currently present in `docker-compose.yml`.

## Key Points
- `docker-compose.yml` is the source of truth for currently implemented services.
- Implemented Docker services:
  - `postgres`: official `postgres` image, credentials from `.env` via `POSTGRES_USER` and `POSTGRES_PASSWORD`, host port `5434` mapped to container port `5432`, named volume `postgres_data`.
  - `kafka`: official `apache/kafka` image, single-node KRaft broker/controller, internal listener `kafka:9092`, host listener `localhost:29092`, controller listener `kafka:9093`, auto topic creation enabled, 168-hour log retention, named volume `kafka_data`.
  - `kafka-ui`: `provectuslabs/kafka-ui`, container name `kafka_ui`, depends on Kafka, host port `8081` mapped to container port `8080`, connects to Kafka through Docker networking at `kafka:9092`.
  - `python-sse`: custom image built from `event-stream/Dockerfile`, depends on Kafka, passes `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` and `KAFKA_TOPIC=wikimedia.recentchange.raw`.
- Kafka listener setup reflects a previous fix: Docker-internal clients use `kafka:9092`, while host-side clients use `localhost:29092`. This avoids Kafka UI being redirected to `localhost` inside its own container.
- Current Kafka topic name follows dotted-domain style: `wikimedia.recentchange.raw`.
- `AGENTS.md` says to validate producer changes with Docker Compose and topic checks, but no such runtime validation was performed in this catch-up session.

## Issues Encountered
- The worktree was already dirty before this log was written:
  - Modified: `docker-compose.yml`
  - Modified: `event-stream/src/producer.py`
  - Untracked: `AGENTS.md`
  - Untracked: `codex-sessions/`
- One parallel attempt to read `README.md` hit a sandbox bind issue referencing a missing `.agents` path, but a retry succeeded. The README only contains a short title/description.
- No Docker daemon checks were run, so this session describes file configuration, not confirmed live container state.

## Debugging / Investigation Steps
- Read the `log-session` skill instructions from `/home/nacht29/.codex/skills/log-session/SKILL.md`.
- Confirmed Git root:
  - `git rev-parse --show-toplevel`
- Listed repository files:
  - `rg --files`
- Inspected core configuration and code:
  - `docker-compose.yml`
  - `event-stream/src/producer.py`
  - `event-stream/Dockerfile`
  - `event-stream/requirements.txt`
  - `postgresql/recentchange.sql`
  - `postgresql/test-script.sql`
  - `kafka/commands.sh`
  - `kafka/debug.sh`
  - `kafka/error-fix.md`
  - `docs/Execution_Plan.md`
  - `docs/Execution_Plan_2.md`
  - `docs/Execution_Plan_3.md`
  - Existing session log `codex-sessions/2026-06-18-wikimedia-sse-500.md`
  - `AGENTS.md`
- Checked line-numbered versions of the main implementation files:
  - `nl -ba docker-compose.yml`
  - `nl -ba event-stream/src/producer.py`
  - `nl -ba event-stream/Dockerfile`
  - `nl -ba event-stream/requirements.txt`

## Changes Made
- Created this session log:
  - `codex-sessions/2026-06-18-project-setup-catch-up.md`
- Updated `AGENTS.md` to link this log under `## Codex Session Logs`.
- No application code, Docker service definitions, SQL scripts, or documentation plans were changed.

## New Knowledge / Lessons Learned
- The current Python producer uses `requests_sse.EventSource` to connect to `https://stream.wikimedia.org/v2/stream/recentchange`.
- Producer configuration:
  - `BOOTSTRAP_SERVERS` comes from `KAFKA_BOOTSTRAP_SERVERS`, defaulting to `localhost:29092` for host-side local runs.
  - `KAFKA_TOPIC` comes from `KAFKA_TOPIC`, defaulting to `wikimedia.recentchange.raw`.
  - Docker overrides those defaults so the producer connects to Kafka at `kafka:9092` inside the Compose network.
- Producer behavior:
  - Registers `SIGINT` and `SIGTERM` handlers that set a global shutdown flag.
  - Creates a Confluent Kafka `Producer` with `bootstrap.servers` and `client.id=socket.gethostname()`.
  - Opens the Wikimedia recentchange SSE stream with a custom `User-Agent`.
  - Iterates over SSE events and ignores non-`message` events.
  - Parses `event.data` as JSON.
  - Skips events where `change['meta']['domain'] == 'canary'`.
  - Serializes the full event back to JSON and produces it to Kafka.
  - Calls `producer.poll(0)` on each loop to drain delivery callbacks.
  - Logs delivery success with topic, partition, and offset, or delivery failure with Kafka error code/name/detail.
  - In `finally`, flushes the producer and logs whether messages failed to flush.
- Producer limitations to keep in mind:
  - The code assumes parsed events contain `meta.domain`; malformed but valid JSON without that path could raise `KeyError`.
  - The top-level exception handler only catches `KeyboardInterrupt`, `RuntimeError`, and `TypeError`.
  - A previous session documented that `requests_sse.client.InvalidStatusCodeError` from a Wikimedia HTTP 500 is currently not handled and can stop the producer.
  - There is no reconnect/backoff loop around the SSE stream yet.
- PostgreSQL SQL files are early scaffolding:
  - `postgresql/recentchange.sql` creates schemas named `"wikimedia.raw_eventstream"` and `"wikimedia.recentchange"` and has placeholder table DDL commented out.
  - `postgresql/test-script.sql` creates `"wikimedia.test.test_table"`, inserts two rows idempotently, and selects from it.
- `README.md` is minimal and does not yet document the actual Compose setup.
- The execution plans are useful for direction, but some details are target-state only and should not be treated as already built.

## Open Questions / Follow-ups
- Pin Docker image versions instead of using floating `postgres` and `apache/kafka` tags, as recommended in `docs/Execution_Plan_3.md`.
- Decide whether the Postgres volume path should be adjusted to a standard image data path. Current Compose maps `postgres_data` to `/var/lib/postgresql/wikimedia-analytics`.
- Add retry/backoff handling for retryable Wikimedia SSE/request failures, especially the previously observed HTTP 500 `InvalidStatusCodeError`.
- Consider guarding producer event parsing with safer access to `meta.domain`.
- Add Spark, MinIO, Airflow, and downstream storage only after the current ingestion baseline is stable and validated.
- Expand `README.md` to explain how to start the stack, inspect Kafka, view Kafka UI, and understand the implemented versus planned architecture.

## Useful References
- `docker-compose.yml`
- `event-stream/src/producer.py`
- `event-stream/Dockerfile`
- `event-stream/requirements.txt`
- `kafka/error-fix.md`
- `kafka/commands.sh`
- `postgresql/recentchange.sql`
- `postgresql/test-script.sql`
- `docs/Execution_Plan_3.md`
- `codex-sessions/2026-06-18-wikimedia-sse-500.md`
- `docker compose up -d`
- `docker compose up -d kafka kafka-ui`
- `docker compose logs --tail=120 python-sse`
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list`
