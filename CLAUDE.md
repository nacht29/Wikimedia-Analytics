# CLAUDE.md — Wikimedia-Analytics

<!-- PROTECTED -->
## Project Goal

Perform clickstream analysis and traffic analysis on Wikimedia projects using public Wikimedia data sources, simulating the kind of website analytics done on consumer platforms (e.g. e-commerce, media sites).

**Data sources:**

* **Wikimedia EventStreams** (SSE) — real-time edit/recentchange events, used as a proxy for live user activity and streaming ingestion practice
* **Wikimedia Pageviews dumps** — page-level reader traffic volume data for traffic analysis
* **Wikimedia monthly clickstream TSV dumps** — reader navigation data for batch analytics

Note: EventStreams carries edit/recentchange events, not reader navigation. Pageviews and Clickstream dumps are separate batch sources used in analytics phases.

**Expected final product:**

* A working raw → modelled analytics pipeline for Wikimedia data
* Streaming ingestion from EventStreams into Kafka and PostgreSQL
* Batch ingestion for Pageviews and Clickstream dumps into PostgreSQL
* Modelled tables for edits, page traffic, and reader navigation
* Analytics outputs for traffic trends, popular pages, navigation paths, and activity patterns
* Orchestration and visualisation may be added later, but are not required at the current stage
<!-- /PROTECTED -->

---

## Stack

| Layer            | Technology                                                   |
| ---------------- | ------------------------------------------------------------ |
| Language         | Python, Shell                                                |
| Containerisation | Docker / Docker Compose                                      |
| Message broker   | Kafka (KRaft mode, no ZooKeeper)                             |
| Database         | PostgreSQL                                                   |
| SSE consumer     | Python (`python-sse` service)                                |
| Kafka UI         | provectuslabs/kafka-ui (port 8081)                           |
| Orchestration    | None yet; manual scripts first, Airflow later only if needed |

---

## Repo Layout

```text
Wikimedia-Analytics/
├── event-stream/       # Python SSE consumer (Dockerised)
├── kafka/              # Kafka config / topic setup
├── postgresql/         # Schema, migrations, seed scripts
├── docs/               # Design notes and references
└── docker-compose.yml  # Brings up postgres, kafka, kafka-ui, python-sse
```

Note: repo structure is expected to change as batch ingestion, modelling, orchestration, and analytics layers are added.

---

<!-- PROTECTED -->
## Branching Strategy

* `staging` — active development and testing
* `main` — stable releases only; each milestone merged as a **single commit** to keep history clean
<!-- /PROTECTED -->

---

<!-- PROTECTED -->
## Code Style

Observed from `event-stream/src/producer_prod.py` — follow these conventions in new/modified code:

* **Indentation:** tabs, not spaces.
* **Type hints:** inline on function params, e.g. `def log(msg:str):` — no space before the colon.
* **Logging:** no logging framework; plain `print(f"{datetime.datetime.now()}\t|\t{msg}", flush=True)`. Timestamp + tab-pipe-tab is the standard log line format — keep it consistent for any new log line.
* **No classes** — procedural/functional style; module-level functions and constants only.
* **Config:** all tunables sourced via `os.getenv("NAME", "default")` and cast to the correct type (`int(...)`, `float(...)`) at the top of the module. Never hardcode a tunable inline.
* **Naming:** `snake_case` for functions/variables, `UPPER_SNAKE_CASE` for module-level constants/config.
* **Comments:** terse, inline, explain *why* not *what* (e.g. why a `poll()`/`flush()` call is needed). No docstrings.
* **Error handling:** catch specific exceptions, never bare `except:`. Retry/backoff is hand-rolled (no retry library), exponential via `wait = min(wait * 2, MAX_RETRY_WAIT)`.
* **Shutdown:** SIGINT/SIGTERM handled via a module-level `shutdown` flag set in a signal handler, checked inside loop bodies — not exceptions.
<!-- /PROTECTED -->

---

## Architecture (current → target)

```text
Wikimedia EventStreams (SSE)
        │
        ▼
  python-sse consumer
        │  exponential backoff + Last-Event-ID resumption
        ▼
     Kafka topic
        │  at-least-once delivery; dedup on meta.id
        ▼
    PostgreSQL raw tables
        │  upserts keyed on meta.id
        ▼
  Analytics / Transformation
```

Batch sources are added later:

```text
Wikimedia Pageviews dumps / Clickstream dumps
        │
        ▼
  batch ingestion scripts
        │
        ▼
    PostgreSQL raw tables
        │
        ▼
  Analytics / Transformation
```

**Gap recovery (target, design validated 2026-06-30, not yet implemented):** two-regime model.
Short/medium gaps (< ~7 days) resolve via SSE `Last-Event-ID` resumption on the live stream — no
separate stream needed. Long gaps or deliberate restarts use a two-stream model: the live producer
restarts from current position and a separate `backfill.py` script fills `last_event_id →
restart_event_id` into its own Kafka topic (`wikimedia.recentchange.backfill`), tracked via a
`backfill_queue.json` gap list. Live producer checkpoints (`resume_state.json`) are written in the
Kafka delivery callback, not on SSE read. See [session log](claude-sessions/2026-06-30-sse-retry-recovery-backfill-design.md).

---

<!-- PROTECTED -->
## Delivery Guarantees & Key Principles

* **At-least-once delivery** is the target throughout the streaming pipeline.
* **Deduplication** via `meta.id` — idempotent Kafka producers and PostgreSQL upserts.
* **SSE resumption** uses persistent `Last-Event-ID`; reconnect is the steady-state loop, not an exception path.
* **No curl-based health checks** — reconnect directly with exponential backoff + jitter.
* **Wikimedia SSE quirk:** server enforces a ~15-minute connection timeout; handle transparently in the reconnect loop.
* **Logging:** dual output — stdout (visible via `docker logs`) and a rotating file handler on a mounted volume.
* **Alerting:** dead-man's-switch / heartbeat pattern; the failing process does not attempt to send its own alert.
* **Raw-first ingestion:** keep full raw payloads/files first, then model into curated tables later.
* **Keep tooling simple first:** no Spark or Airflow at this stage unless the pipeline complexity justifies it.
<!-- /PROTECTED -->

---

## Project Phases

1. **Resilient ingestion** ← current focus (retry logic, recovery, logging, alerting)
2. End-to-end pipeline plumbing with delivery guarantees
3. Batch ingestion for Pageviews and Clickstream dumps
4. Data modelling
5. Analytics and transformation
6. Orchestration and observability
7. Serving and visualisation
8. Hardening

---

## Related Resources

* **Obsidian notes:** `github.com/nacht29/Obsidian` → `Projects/`
* **Wikimedia EventStreams docs:** https://stream.wikimedia.org
* **Wikimedia recentchange schema:** https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml
* **Wikimedia pageviews dumps:** https://dumps.wikimedia.org/other/pageviews/
* **Wikimedia clickstream dumps:** https://dumps.wikimedia.org/other/clickstream/

---

## Session Log

> Recent sessions are listed below. Full summaries in `claude-sessions/`.
> At the start of each session, scan this list for relevant prior context.

| Date | Summary | File |
|------|---------|------|
| 2026-07-04 | Verified producer_prod.py against real disconnect-test logs; found 4 bugs (FAILED_RETRY silent death, skippable flush() on shutdown, JSON-parse except-ordering bug, missing sys.exit() parens) | [link](claude-sessions/2026-07-04-sse-retry-error.md) |
| 2026-07-04 | Brainstormed retry/resumption-only design (scoped out backfill.py); resolved single-checkpoint vs multi-ID question; in progress | [link](claude-sessions/2026-07-04-sse-retry-resumption-design-brainstorm.md) |
| 2026-06-30 | Designed two-regime SSE retry/recovery + backfill architecture; no code written | [link](claude-sessions/2026-06-30-sse-retry-recovery-backfill-design.md) |


