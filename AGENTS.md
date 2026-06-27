# Repository Guidelines

## Project Structure & Module Organization

This repository is a Docker-based Wikimedia analytics pipeline. Service definitions live in `docker-compose.yml`. The Python Wikimedia Server-Sent Events producer is in `event-stream/src/producer.py`, with its container setup in `event-stream/Dockerfile` and dependencies in `event-stream/requirements.txt`. Kafka helper commands and incident notes are stored in `kafka/`. PostgreSQL schema and test SQL scripts live in `postgresql/`. Planning notes are kept in `docs/`. Example event payloads are in `event-stream/examples/`.

## Build, Test, and Development Commands

- `docker compose up -d`: build and start PostgreSQL, Kafka, Kafka UI, and the SSE producer.
- `docker compose up -d kafka kafka-ui`: start only Kafka and the Kafka UI for broker debugging.
- `docker compose logs --tail=120 python-sse`: inspect recent producer output and delivery callbacks.
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list`: list Kafka topics from inside the broker container.
- `docker compose down`: stop local services while preserving named volumes.

Kafka UI is exposed at `http://localhost:8081`. PostgreSQL maps host port `5434` to container port `5432`.

## Coding Style & Naming Conventions

Use Python 3 style for producer code: `snake_case` for functions and variables, uppercase constants such as `KAFKA_TOPIC`, and environment-driven settings for deployable options. Keep logging direct and flush long-running producer output so Docker logs remain useful. Prefer concise shell scripts with `#!/usr/bin/env bash` and `set -euo pipefail`. Name Kafka topics with dotted domains, for example `wikimedia.recentchange.raw`.

## Testing Guidelines

There is no formal test framework configured yet. Validate changes with Docker Compose and targeted container checks. For producer changes, start Kafka and the producer, then confirm messages appear in `docker compose logs python-sse` and topics are visible with the topic-list command. For SQL changes, keep executable examples in `postgresql/test-script.sql` or a clearly named companion script.

## Commit & Pull Request Guidelines

The repository uses short Conventional Commit-style prefixes, especially `feat:` and `chore:`. Keep commit subjects imperative and specific, for example `feat: Dockerise Python SSE producer`. Pull requests should explain the pipeline component changed, include local validation commands, mention any `.env` or Docker configuration impact, and attach screenshots only when UI behavior changes.

## Security & Configuration Tips

Keep secrets and machine-specific values in `.env`; do not hard-code credentials in Python, SQL, or Compose files. When adding services, prefer Docker network hostnames such as `kafka:9092` for container-to-container traffic and reserve `localhost` addresses for host access.

## Project Context

Project contexts, notes and updates can be cross-referenced here: [Obsidian Vault note](https://github.com/nacht29/Obsidian/blob/main/Projects/Wikimedia-Analytics.md)

## Codex Session Logs

- [2026-06-24 — RecentChange Backfill Recovery Plan](codex-sessions/2026-06-24-recentchange-backfill-recovery-plan.md)
- [2026-06-24 — Docker SSE Shutdown Hang](codex-sessions/2026-06-24-docker-sse-shutdown-hang.md)
- [2026-06-24 — Producer Network Drop Retry Fix](codex-sessions/2026-06-24-producer-network-drop-retry-fix.md)
- [2026-06-18 — Project Setup Catch-Up](codex-sessions/2026-06-18-project-setup-catch-up.md)
- [2026-06-18 — Wikimedia SSE 500 Incident](codex-sessions/2026-06-18-wikimedia-sse-500.md)
