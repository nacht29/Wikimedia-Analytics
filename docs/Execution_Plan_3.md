# WikiPulse
## Revised Execution Plan (Docker-First, KRaft, Current Stack)

**Version:** POC v1.2  
**Date:** 2026-03-22  
**Primary goal:** Build a portfolio-grade, reproducible data engineering platform using **Docker Compose, PostgreSQL, Apache Kafka (KRaft), Apache Spark Structured Streaming, Apache Airflow, MinIO, and Power BI** on top of **real Wikimedia public data**.

---

## 1. Executive Summary

WikiPulse is a **hybrid streaming + batch analytics platform** built on Wikimedia public datasets.

It ingests:
1. **Wikimedia EventStreams (`recentchange`)** for live editorial activity
2. **Wikimedia Pageviews API** for article readership metrics
3. **Wikimedia Clickstream dumps** for page-to-page navigation behavior

It processes and serves data through this architecture:

```text
Wikimedia EventStreams --> Python producer --> Kafka (KRaft)
                                              |
                                              v
                                   Spark Structured Streaming
                                              |
                                   +----------+----------+
                                   |                     |
                                   v                     v
                              MinIO Bronze          MinIO Silver

Wikimedia Pageviews API --> Airflow DAG --> MinIO Bronze/Silver
Wikimedia Clickstream   --> Airflow DAG --> MinIO Bronze/Silver

MinIO Silver/Gold --> Spark batch / SQL transforms --> PostgreSQL marts --> Power BI
```

The project is intentionally **Docker-first** and **single-node local** for development, but uses production-style patterns:
- message broker instead of direct ingestion
- object storage instead of writing everything straight into a database
- orchestration separated from processing
- curated serving marts for BI

---

## 2. Design Principles

1. **Build in strict phases** so debugging stays manageable.
2. **Pin image versions** in Docker Compose to avoid silent upstream breakage.
3. **Use current defaults** instead of legacy patterns:
   - Kafka in **KRaft mode**
   - no ZooKeeper for new deployments
   - PostgreSQL mounted using the newer parent directory layout when using Postgres 18+
4. **Validate each service in isolation** before integrating it into the full pipeline.
5. **Keep the first version demoable**, not over-engineered.

---

## 3. Final Technology Stack

| Layer | Tool | Role |
|---|---|---|
| Streaming source | Wikimedia EventStreams | live recent changes |
| Batch source 1 | Wikimedia Pageviews API | daily article readership |
| Batch source 2 | Wikimedia Clickstream dumps | monthly traffic relationships |
| Event broker | Apache Kafka (KRaft) | decouples ingestion from processing |
| Stream processing | Apache Spark Structured Streaming | parsing, normalization, rolling aggregates |
| Orchestration | Apache Airflow | scheduled ingestion and downstream jobs |
| Object storage | MinIO | local S3-compatible lake storage |
| Serving database | PostgreSQL | SQL mart layer for BI |
| BI layer | Power BI | reporting and dashboarding |
| Container orchestration | Docker Compose | reproducible local platform |

---

## 4. Updated Infrastructure Decisions

### 4.1 PostgreSQL
Use the official Postgres image, but **pin the major version**.

Recommended:
- `postgres:18`
- mount volume to `/var/lib/postgresql`

Reason:
- Postgres 18+ changed the image volume layout and `PGDATA` behavior.
- Using a pinned major version prevents confusing breakage when `latest` changes.

### 4.2 Kafka
Use the official Apache Kafka Docker image in **KRaft mode**.

Recommended:
- `apache/kafka:4.1.x`
- no ZooKeeper
- single-node broker/controller for local development

Reason:
- KRaft is the modern Kafka metadata mode.
- ZooKeeper is no longer the preferred deployment path and is removed in Kafka 4.0+.

### 4.3 Airflow
Use the official Airflow image and Docker Compose only for **local development / learning / POC work**.

Recommended:
- `apache/airflow:3.1.x`

Reason:
- Airflow in Docker Compose is good for local development.
- It should not be treated as the final production design; this project only needs the local dev pattern.

### 4.4 Spark
Use Apache Spark 4.x for current APIs and current Structured Streaming behavior.

Recommended:
- Spark 4.1.x compatible image or a custom image based on a current Spark distribution

Reason:
- The project is stream-centric.
- Spark 4.x keeps the platform modern and aligned with current documentation.

---

## 5. Data Sources

### 5.1 Source A — Wikimedia EventStreams
**Type:** streaming SSE  
**Endpoint:** `https://stream.wikimedia.org/v2/stream/recentchange`

Use cases:
- detect pages with edit spikes
- analyze bot vs human edit mix
- compare editorial activity with pageview demand

### 5.2 Source B — Wikimedia Pageviews API
**Type:** batch API  
**Base pattern:** per-article pageview requests

Use cases:
- fetch daily readership for selected articles
- compare pageviews with edits
- compute editorial attention vs reader demand

### 5.3 Source C — Wikimedia Clickstream Dumps
**Type:** monthly batch files  
**Base index:** clickstream dump directory

Use cases:
- build source-destination traffic edges
- enrich article-level metrics with inbound and outbound relationships
- support navigation-focused dashboards

---

## 6. Target Architecture

```text
                    +---------------------------------+
                    | Wikimedia EventStreams          |
                    | recentchange SSE                |
                    +----------------+----------------+
                                     |
                                     v
                    +---------------------------------+
                    | Wikimedia Producer (Python)     |
                    | SSE -> JSON -> Kafka topic      |
                    +----------------+----------------+
                                     |
                                     v
                         +--------------------------+
                         | Kafka (KRaft mode)       |
                         | topic: recentchange_raw  |
                         +------------+-------------+
                                      |
                                      v
               +---------------------------------------------+
               | Spark Structured Streaming                  |
               | parse -> normalize -> enrich -> aggregate   |
               +----------------------+----------------------+
                                      |
                        +-------------+-------------+
                        |                           |
                        v                           v
             +---------------------+     +----------------------+
             | MinIO Bronze        |     | MinIO Silver         |
             | raw event storage   |     | cleaned datasets     |
             +---------------------+     +----------------------+

    +---------------------+         +-----------------------------+
    | Wikimedia Pageviews |         | Wikimedia Clickstream Dumps |
    +----------+----------+         +---------------+-------------+
               |                                      |
               v                                      v
        +--------------+                      +--------------+
        | Airflow DAG  |                      | Airflow DAG  |
        +------+-------+                      +------+-------+
               |                                     |
               +------------------+------------------+
                                  |
                                  v
                  +-------------------------------+
                  | Spark batch / SQL transforms  |
                  | gold models / serving marts   |
                  +---------------+---------------+
                                  |
                                  v
                         +----------------+
                         | PostgreSQL mart |
                         +--------+-------+
                                  |
                                  v
                            +-----------+
                            | Power BI  |
                            +-----------+
```

---

## 7. Logical Data Architecture

### Bronze
- `bronze_recentchange`
- `bronze_pageviews`
- `bronze_clickstream`

### Silver
- `silver_edit_events`
- `silver_article_pageviews`
- `silver_clickstream_edges`
- `silver_article_daily_metrics`

### Gold
- `gold_article_activity_daily`
- `gold_attention_gap_daily`
- `gold_clickstream_top_edges`
- `gold_wiki_activity_summary`

### Serving
- PostgreSQL `mart` schema for Power BI

---

## 8. Repository Structure

```text
wiki-pulse/
├── README.md
├── .env
├── docker-compose.yml
├── docs/
│   ├── execution-plan.md
│   ├── architecture.md
│   └── references.md
├── data/
│   ├── postgres/
│   ├── kafka/
│   └── minio/
├── ingestion/
│   └── wikimedia_producer/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── producer.py
├── spark/
│   ├── Dockerfile
│   ├── streaming/
│   │   └── recentchange_stream.py
│   └── batch/
│       ├── pageviews_job.py
│       ├── clickstream_job.py
│       └── gold_marts_job.py
├── airflow/
│   ├── dags/
│   │   ├── pageviews_ingestion_dag.py
│   │   ├── clickstream_ingestion_dag.py
│   │   └── gold_build_dag.py
│   ├── plugins/
│   └── requirements.txt
├── sql/
│   ├── ddl/
│   ├── marts/
│   └── checks/
└── schemas/
    ├── recentchange_schema.json
    ├── pageviews_schema.json
    └── clickstream_schema.json
```

---

## 9. Current Baseline Infrastructure

Your current starting point is now:
- PostgreSQL in Docker
- Kafka in Docker using **KRaft**, not ZooKeeper

That changes the project baseline.

The first milestone is no longer “stand up everything at once.”

It is now:

```text
PostgreSQL + Kafka(KRaft) up and validated locally
```

This is the correct baseline because it proves:
- Docker networking basics
- stateful volumes
- a serving database
- a modern streaming broker

---

## 10. Revised Execution Phases

### Phase 0 — Freeze Scope and Tooling

#### Goal
Lock the project architecture and current tool choices before writing more code.

#### Decisions to freeze
- Docker Compose for local orchestration
- PostgreSQL as serving mart database
- Kafka in KRaft mode
- Spark for stream and batch processing
- Airflow for scheduled batch orchestration
- MinIO as local object storage
- Power BI as the BI layer

#### Exit criteria
- stack decisions are stable
- image versions are pinned
- no unresolved infra choices remain

---

### Phase 1 — Core Infrastructure Foundation

#### Goal
Stand up the minimum core platform that all later work depends on.

#### Scope
- PostgreSQL
- Kafka (KRaft)

#### Tasks
1. Pin Docker image versions
2. Finalize `.env`
3. Finalize `docker-compose.yml`
4. Configure persistent volumes
5. Verify service startup and restart behavior
6. Verify logs and local connectivity

#### Outputs
- working `docker-compose.yml`
- `.env`
- local persistent volumes

#### Exit criteria
- Postgres starts cleanly
- Kafka starts cleanly in KRaft mode
- both services survive restart
- logs show healthy startup behavior

---

### Phase 2 — Kafka Validation Before Real Ingestion

#### Goal
Prove Kafka is usable before building the Wikimedia producer.

#### Scope
- topic creation
- produce/consume roundtrip
- payload inspection

#### Tasks
1. Create a test topic
2. Produce a small JSON message
3. Consume the same message
4. Confirm advertised listeners work for both host and future container clients
5. Decide the initial topic naming convention

#### Recommended initial topics
- `test_topic`
- `recentchange_raw`

#### Exit criteria
- a message can be written and read successfully
- topic metadata is correct
- no listener or bootstrap confusion remains

#### Important note
At this phase, **do not build Kafka sink connectors**. This project does not need Kafka Connect first. The next downstream consumer will be your custom Python producer and later Spark.

---

### Phase 3 — MinIO Object Storage

#### Goal
Prepare the data lake sink before introducing Spark.

#### Scope
- MinIO container
- bucket creation
- S3-compatible credentials and connectivity

#### Tasks
1. Add MinIO service to Docker Compose
2. Mount persistent storage
3. Create buckets:
   - `wikidata-bronze`
   - `wikidata-silver`
   - `wikidata-gold` (optional)
4. Verify UI and access keys
5. Test local write/read

#### Exit criteria
- MinIO starts reliably
- buckets exist
- credentials work from host and containers

---

### Phase 4 — Wikimedia Producer

#### Goal
Ingest real-time Wikimedia `recentchange` events into Kafka.

#### Scope
- Python SSE client
- JSON validation
- publish to Kafka topic

#### Tasks
1. Build `wikimedia_producer` container
2. Subscribe to `recentchange`
3. Parse and validate payloads safely
4. Send raw JSON to Kafka topic `recentchange_raw`
5. Add retry and reconnect logic
6. Add minimal logging

#### Outputs
- producer Dockerfile
- producer code
- live events in Kafka

#### Exit criteria
- live events continuously reach Kafka
- transient disconnects do not kill ingestion
- sample payloads are inspectable and stable enough for downstream parsing

---

### Phase 5 — Spark Streaming Pipeline

#### Goal
Consume Kafka events and write Bronze/Silver datasets to MinIO.

#### Scope
- Kafka source in Spark Structured Streaming
- JSON parsing and schema normalization
- Parquet output

#### Tasks
1. Build or select a Spark image compatible with Kafka integration
2. Define schema for recentchange events
3. Read from `recentchange_raw`
4. Parse raw JSON into structured columns
5. Standardize fields such as:
   - event timestamp
   - wiki
   - title
   - user
   - bot flag
   - event type
   - revision delta
6. Write Bronze raw archive and Silver cleaned tables to MinIO
7. Partition by practical keys such as date and wiki

#### Outputs
- `recentchange_stream.py`
- `bronze_recentchange`
- `silver_edit_events`

#### Exit criteria
- Spark job runs continuously
- MinIO receives valid Parquet outputs
- schema is stable enough for batch joins later

---

### Phase 6 — Airflow Local Orchestration Layer

#### Goal
Introduce Airflow only after there are real tasks worth orchestrating.

#### Scope
- Airflow webserver, scheduler, init/bootstrap
- DAG directory mounting
- metadata DB

#### Tasks
1. Add Airflow services to Docker Compose
2. Initialize Airflow metadata database
3. Configure local user and mounts
4. Expose DAGs directory
5. Verify scheduler picks up DAGs

#### Exit criteria
- Airflow UI opens
- scheduler is healthy
- test DAG can run successfully

#### Important note
Airflow is introduced **after** core streaming infra, not before. That keeps debugging surface area under control.

---

### Phase 7 — Pageviews Batch Ingestion

#### Goal
Schedule pageview collection for selected articles.

#### Article selection strategy
Start with one or both:
- most actively edited pages from the recentchange stream
- a curated manual seed list

#### Tasks
1. Build pageviews DAG
2. Fetch daily pageview data
3. Store raw responses in Bronze
4. Normalize to Silver
5. Add idempotent reload logic
6. Add failure handling

#### Outputs
- `pageviews_ingestion_dag.py`
- `bronze_pageviews`
- `silver_article_pageviews`

#### Exit criteria
- DAG runs on schedule
- data is stored correctly
- results can later join to edit activity

---

### Phase 8 — Clickstream Batch Ingestion

#### Goal
Ingest and normalize monthly clickstream dumps.

#### Tasks
1. Build clickstream DAG
2. Download one language edition first, likely `enwiki`
3. Inspect TSV shape
4. Store raw file in Bronze
5. Build normalized edge table with:
   - source page
   - destination page
   - traffic type
   - count
6. Write Silver output

#### Outputs
- `clickstream_ingestion_dag.py`
- `bronze_clickstream`
- `silver_clickstream_edges`

#### Exit criteria
- one monthly dump is processed end to end
- edge table is queryable and reusable

---

### Phase 9 — Gold Modeling and Serving Marts

#### Goal
Build interview-friendly analytics tables.

#### Candidate gold tables
1. `gold_article_activity_daily`
2. `gold_article_pageviews_daily`
3. `gold_attention_gap_daily`
4. `gold_clickstream_top_edges`
5. `gold_wiki_activity_summary`

#### Tasks
1. Define analytical grain for each table
2. Join edits with pageviews
3. Compute metrics such as:
   - edit count
   - unique editors
   - avg size delta
   - pageviews
   - edits per 1000 pageviews
   - attention gap
4. Load serving tables into PostgreSQL
5. Add indexes
6. Add simple monitoring / validation checks

#### Exit criteria
- PostgreSQL marts are Power BI-ready
- table definitions are documented
- query performance is acceptable for demo use

---

### Phase 10 — Power BI Reporting Layer

#### Goal
Build clean, portfolio-ready dashboards.

#### Suggested report pages
1. **Streaming Activity Overview**
2. **Reader Demand vs Editorial Attention**
3. **Clickstream Navigation**
4. **Trend Monitoring**

#### Tasks
1. Connect Power BI to PostgreSQL
2. Import serving tables
3. Create semantic measures where useful
4. Keep visuals focused and interview-friendly
5. Capture screenshots for README

#### Exit criteria
- dashboards refresh from PostgreSQL
- visuals tell a clear story
- report is demoable without hand-waving

---

### Phase 11 — Hardening and Documentation

#### Goal
Make the repo reproducible and credible.

#### Tasks
1. Add row count and freshness checks
2. Add basic null and duplicate checks
3. Document startup order
4. Write final README
5. Add reset / teardown instructions
6. Record known limitations

#### Exit criteria
- project can be cloned and understood by another person
- startup steps are deterministic
- the demo story is clear

---

## 11. Revised Build Order

```text
1. PostgreSQL + Kafka(KRaft)
2. Kafka topic validation
3. MinIO
4. Wikimedia producer
5. Spark streaming -> MinIO
6. Airflow
7. Pageviews batch DAG
8. Clickstream batch DAG
9. Gold models -> PostgreSQL
10. Power BI
11. Documentation and polish
```

This order is intentionally different from the runtime data flow.

It is optimized for:
- easier debugging
- earlier proof of modern Kafka setup
- lower integration risk

---

## 12. Analytics Questions the Project Should Answer

### Streaming / editorial activity
- Which pages receive the most edits in the last hour/day?
- Which wiki editions are most active?
- What share of edits are bot-generated?

### Reader demand
- Which monitored pages receive the most pageviews?
- Are highly viewed pages also highly edited?

### Editorial attention vs demand
- Which pages are over-read but under-edited?
- Which pages are over-edited relative to readership?
- Which pages are balanced between demand and attention?

### Navigation
- Which pages send the most traffic into selected articles?
- Which destinations do readers visit next?

### Trend monitoring
- Which pages show simultaneous edit and pageview spikes?
- Which topics appear to reflect fast-moving public attention?

---

## 13. Storage Design

### MinIO buckets
- `wikidata-bronze`
- `wikidata-silver`
- `wikidata-gold` (optional)

### PostgreSQL schemas
- `staging`
- `mart`
- `monitoring` (optional)

### Example object layout

```text
wikidata-bronze/
  recentchange/dt=2026-03-22/wiki=enwiki/part-*.parquet
  pageviews/dt=2026-03-22/article=Artificial_intelligence/*.json
  clickstream/month=2026-02/wiki=enwiki/*.tsv

wikidata-silver/
  edit_events/dt=2026-03-22/wiki=enwiki/*.parquet
  article_pageviews/dt=2026-03-22/*.parquet
  clickstream_edges/month=2026-02/wiki=enwiki/*.parquet
```

---

## 14. Updated Risks and Mitigations

| Risk | Why it matters | Mitigation |
|---|---|---|
| Unpinned Docker images | silent behavior changes | pin major/minor versions |
| Kafka listener confusion | producers/consumers fail to connect | validate advertised listeners early |
| Adding Airflow too early | high debugging overhead | add Airflow only after core pipeline exists |
| Spark + Kafka dependency mismatch | stream job fails at runtime | pin Spark-compatible packages and test locally |
| Pageviews scope too broad | unnecessary API churn | start with a small monitored article set |
| Clickstream files too large | slow local iteration | start with one language and one month |
| Over-modeling PostgreSQL | slows project completion | keep marts narrow and demo-focused |

---

## 15. Definition of Success

The POC is successful when:
- Docker Compose starts the platform reliably
- Kafka receives real Wikimedia events
- Spark writes cleaned event data to MinIO
- Airflow runs pageviews and clickstream ingestion jobs on schedule
- curated marts are available in PostgreSQL
- Power BI can answer the project’s core analytics questions

---

## 16. Immediate Next Step

Do **not** jump to Airflow yet.

The next concrete milestone is:

```text
Validate Kafka in KRaft mode with one topic and one produce/consume roundtrip.
```

After that:

```text
Add MinIO, then build the Wikimedia producer.
```

That sequencing keeps the project aligned with current tooling while minimizing debugging noise.

---

## 17. Reference Notes

Use official docs first whenever behavior is ambiguous:
- Apache Kafka Docker and upgrade docs for KRaft and current images
- Docker Official Postgres image docs for version-specific volume behavior
- Apache Airflow Docker quickstart and Docker image docs
- Apache Spark docs for Structured Streaming
- Wikimedia EventStreams and MediaWiki docs for recentchange stream behavior
- Microsoft docs for Power BI / Power Query PostgreSQL connectivity

