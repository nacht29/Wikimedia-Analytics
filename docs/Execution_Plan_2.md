# WikiPulse
## Docker-Only Execution Plan
### Real-Time Wikimedia Editorial & Reader-Attention Analytics Platform

**Version:** POC v1  
**Primary goal:** Build a portfolio-worthy end-to-end data engineering system using **Docker, Kafka, Spark, Airflow, PostgreSQL, Power BI**, and Wikimedia public data.

---

## 1. Executive Summary

WikiPulse is a **hybrid streaming + batch** analytics platform built on **real Wikimedia data**.

The project ingests:
1. **Wikimedia EventStreams (`recentchange`)** for live edit/change activity
2. **Wikimedia Pageviews API** for article readership metrics
3. **Wikimedia Clickstream dumps** for page-to-page traffic patterns

The system processes streaming events with **Kafka + Spark Structured Streaming**, stores raw/intermediate data in **object storage**, orchestrates scheduled enrichment with **Airflow**, builds curated marts in **PostgreSQL**, and serves dashboards to **Power BI**.

This execution plan is intentionally **Docker-only** to minimize infrastructure complexity while still demonstrating strong mid-level data engineering practices.

---

## 2. Project Objectives

### 2.1 Technical Objectives
- Build a reproducible multi-service local platform with Docker Compose
- Implement one **real-time streaming pipeline**
- Implement two **batch ingestion/enrichment pipelines**
- Store raw and intermediate data in object storage
- Load curated analytics tables into PostgreSQL
- Connect Power BI to PostgreSQL for reporting

### 2.2 Learning Objectives
- Learn **Kafka** as a buffer and decoupling layer
- Learn **Spark Structured Streaming** for event processing
- Learn **Airflow** for orchestration and batch scheduling
- Learn containerized service integration with **Docker Compose**
- Practice data lake -> serving mart architecture
- Practice analytical modeling for BI consumption

### 2.3 Portfolio Objectives
- Demonstrate a real external streaming source
- Show infrastructure reproducibility
- Show separation of raw, transformed, and serving layers
- Produce meaningful business-style analytics and dashboards

---

## 3. Final Technology Stack

| Layer | Tool | Why |
|---|---|---|
| Streaming source | Wikimedia EventStreams | real-time public event feed |
| Batch source 1 | Wikimedia Pageviews API | article-level readership data |
| Batch source 2 | Wikimedia Clickstream dumps | navigation relationships |
| Event buffer | Apache Kafka | decouples ingestion from processing |
| Stream processing | Apache Spark Structured Streaming | scalable parsing and aggregation |
| Orchestration | Apache Airflow | scheduling, retries, dependency control |
| Object storage | MinIO (S3-compatible) | low-cost local S3-style storage |
| Serving / analytics DB | PostgreSQL | clean SQL serving layer for BI |
| BI layer | Power BI | relevant to Malaysia/Singapore market |
| Containerization | Docker Compose | keeps the POC deployable locally |

---

## 4. Data Sources

### 4.1 Source A — Wikimedia EventStreams
**Type:** Streaming  
**Use case:** live edit/change activity  
**Primary endpoint:** `https://stream.wikimedia.org/v2/stream/recentchange`

Example use:
- detect trending pages by edit count
- compare edit spikes with pageview spikes
- identify active wikis and editors

Key characteristics:
- continuous feed
- semi-structured JSON event payloads
- best suited for Kafka ingestion

---

### 4.2 Source B — Wikimedia Pageviews API
**Type:** Batch API  
**Use case:** article readership / demand metrics  
**Base reference:** `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article`

Example use:
- fetch daily pageviews for curated article lists
- compare readership vs editorial activity
- compute attention gap metrics

Key characteristics:
- scheduled collection
- API-request based
- clean fit for Airflow DAGs

---

### 4.3 Source C — Wikimedia Clickstream Dumps
**Type:** Batch file dump  
**Use case:** navigation relationships between pages  
**Base reference:** `https://dumps.wikimedia.org/other/clickstream/`

Example use:
- build page-to-page traffic graph
- identify common inbound/outbound paths
- enrich article analytics with top referrers

Key characteristics:
- monthly file releases
- large TSV-style batch ingestion
- best handled with scheduled batch processing

---

## 5. High-Level Architecture

```text
                +----------------------------------+
                | Wikimedia EventStreams           |
                | recentchange SSE                 |
                +----------------+-----------------+
                                 |
                                 v
                   +---------------------------+
                   | Wikimedia Producer        |
                   | Python SSE client         |
                   +-------------+-------------+
                                 |
                                 v
                          +-------------+
                          | Kafka       |
                          | raw topic   |
                          +------+------+ 
                                 |
                                 v
              +--------------------------------------+
              | Spark Structured Streaming           |
              | parse -> normalize -> enrich basic   |
              +------------------+-------------------+
                                 |
                +----------------+----------------+
                |                                 |
                v                                 v
      +--------------------+            +--------------------+
      | MinIO Bronze       |            | MinIO Silver       |
      | raw parquet        |            | cleaned parquet    |
      +--------------------+            +--------------------+

   +------------------------+        +-------------------------------+
   | Wikimedia Pageviews    |        | Wikimedia Clickstream Dumps   |
   | API                    |        | monthly files                 |
   +-----------+------------+        +---------------+---------------+
               |                                     |
               v                                     v
        +-------------+                       +-------------+
        | Airflow DAG |                       | Airflow DAG |
        +------+------+                       +------+------+
               |                                     |
               +------------------+------------------+
                                  |
                                  v
                   +-----------------------------+
                   | Spark batch / SQL modeling  |
                   | gold analytics marts        |
                   +--------------+--------------+
                                  |
                                  v
                          +---------------+
                          | PostgreSQL    |
                          | serving marts |
                          +-------+-------+
                                  |
                                  v
                           +--------------+
                           | Power BI     |
                           | dashboards   |
                           +--------------+
```

---

## 6. Logical Data Architecture

```text
Sources
  ├── EventStreams (stream)
  ├── Pageviews API (batch)
  └── Clickstream dumps (batch)

Bronze Layer
  ├── bronze_recentchange
  ├── bronze_pageviews
  └── bronze_clickstream

Silver Layer
  ├── silver_edit_events
  ├── silver_article_pageviews
  ├── silver_clickstream_edges
  └── silver_article_daily_metrics

Gold Layer
  ├── gold_article_activity_daily
  ├── gold_attention_gap_daily
  ├── gold_clickstream_top_edges
  └── gold_wiki_trend_summary

Serving Layer
  └── PostgreSQL marts for Power BI
```

---

## 7. Container Plan

Target Docker services:

1. `zookeeper`
2. `kafka`
3. `spark-master`
4. `spark-worker`
5. `postgres`
6. `airflow-webserver`
7. `airflow-scheduler`
8. `airflow-init` (one-off / optional)
9. `minio`
10. `wikimedia-producer`

**Expected steady-state runtime:** about **9 containers**  
(`airflow-init` may run once and exit)

### Why this is acceptable
This is complex enough to demonstrate distributed system thinking, but still manageable on a single development machine.

---

## 8. Recommended Repository Structure

```text
wiki-pulse/
├── README.md
├── .env
├── docker-compose.yml
├── docs/
│   ├── execution-plan.md
│   ├── architecture.md
│   └── references.md
├── ingestion/
│   └── wikimedia_producer/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── producer.py
├── spark/
│   ├── streaming/
│   │   ├── Dockerfile
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
│   └── plugins/
├── sql/
│   ├── ddl/
│   ├── marts/
│   └── checks/
├── schemas/
│   ├── recentchange_schema.json
│   ├── pageviews_schema.json
│   └── clickstream_schema.json
└── powerbi/
    └── dashboard_notes.md
```

---

## 9. Execution Phases

### Phase 0 — Planning and Scoping
#### Goal
Freeze project scope before implementation.

#### Deliverables
- finalized architecture
- service list
- source list
- target tables
- dashboard idea list

#### Tasks
- confirm Docker-only setup
- confirm PostgreSQL as serving layer
- confirm Power BI as BI layer
- confirm 3 data sources
- draft high-level metric list

#### Exit Criteria
- architecture is stable enough to begin implementation
- no unresolved stack decisions remain

---

### Phase 1 — Infrastructure Setup
#### Goal
Stand up the minimum viable local platform using Docker Compose.

#### Scope
- Kafka + Zookeeper
- PostgreSQL
- MinIO
- Airflow
- Spark master/worker

#### Tasks
1. Create `.env` with ports, usernames, passwords, bucket names
2. Build `docker-compose.yml`
3. Configure named volumes for:
   - PostgreSQL
   - MinIO
   - Kafka (optional local persistence)
4. Verify inter-container networking
5. Add healthchecks where feasible
6. Initialize Airflow metadata database

#### Outputs
- `docker-compose.yml`
- `.env`
- basic service startup scripts / commands

#### Exit Criteria
- all core services start successfully
- containers can reach each other by service name
- Airflow UI and Kafka broker are reachable
- PostgreSQL and MinIO are healthy

---

### Phase 2 — Wikimedia SSE Streaming Ingestion
#### Goal
Capture live Wikimedia `recentchange` events and publish them into Kafka.

#### Scope
- Python SSE consumer
- Kafka producer logic
- raw topic design

#### Tasks
1. Build `wikimedia-producer` service
2. Subscribe to `recentchange`
3. Parse event payloads safely
4. Filter out malformed / irrelevant events if needed
5. Publish raw JSON to Kafka topic `recentchange_raw`
6. Add logging and basic retry handling

#### Outputs
- producer Dockerfile
- producer Python module
- Kafka raw topic populated with live events

#### Exit Criteria
- live events arrive continuously in Kafka
- producer reconnects after transient failures
- sample events can be inspected from Kafka consumer

#### Questions answered by this phase
- Can the project ingest real-time Wikimedia data reliably?
- What does the raw event schema look like in practice?

---

### Phase 3 — Spark Streaming Pipeline
#### Goal
Consume Kafka events with Spark Structured Streaming and write normalized data into object storage.

#### Scope
- Kafka -> Spark Structured Streaming
- schema parsing
- bronze and silver layer generation

#### Tasks
1. Define Spark schema for `recentchange`
2. Consume Kafka topic
3. Parse JSON payload
4. Standardize fields:
   - event timestamp
   - wiki
   - title
   - user
   - bot flag
   - event type
   - revision length delta
5. Write raw/parsed outputs to MinIO as Parquet
6. Partition by practical keys such as date and wiki

#### Outputs
- `recentchange_stream.py`
- `bronze_recentchange`
- `silver_edit_events`

#### Exit Criteria
- Spark job runs continuously
- Parquet files land in MinIO
- schema is stable enough for downstream work

#### Questions answered by this phase
- Which fields are analytically useful?
- What partitioning strategy is good enough for a POC?

---

### Phase 4 — Pageviews API Batch Ingestion
#### Goal
Set up scheduled ingestion of article pageview data.

#### Scope
- Airflow DAG
- Wikimedia Pageviews API requests
- Bronze/Silver batch datasets

#### Tasks
1. Decide article selection strategy:
   - top edited pages from recentchange
   - curated manual list
   - both
2. Build Airflow DAG for daily pageview pulls
3. Store raw responses in MinIO Bronze
4. Normalize to Silver table structure
5. Include failure handling and idempotent reload logic

#### Outputs
- `pageviews_ingestion_dag.py`
- `bronze_pageviews`
- `silver_article_pageviews`

#### Exit Criteria
- DAG runs on schedule
- pageview data for selected articles is stored
- data can be joined later to edit activity

#### Questions answered by this phase
- Which edited pages are also highly read?
- How does readership change across time windows?

---

### Phase 5 — Clickstream Batch Ingestion
#### Goal
Ingest monthly clickstream dumps for article-to-article navigation analysis.

#### Scope
- download, extract, and transform batch files
- edge-style clickstream modeling

#### Tasks
1. Build Airflow DAG for clickstream download
2. Download selected language dump(s), likely start with `enwiki`
3. Extract and inspect TSV structure
4. Load raw file into Bronze
5. Build normalized edge table:
   - source page
   - destination page
   - traffic type
   - count
6. Persist transformed outputs to Silver

#### Outputs
- `clickstream_ingestion_dag.py`
- `bronze_clickstream`
- `silver_clickstream_edges`

#### Exit Criteria
- monthly clickstream dump is successfully processed
- edge table is queryable
- article-to-article relationships are available for enrichment

#### Questions answered by this phase
- Where do readers come from before reaching a page?
- Which destinations receive the most traffic from selected pages?

---

### Phase 6 — Analytics Modeling
#### Goal
Transform streaming and batch datasets into curated analytics tables.

#### Scope
- Spark batch jobs and/or SQL-based mart builds
- PostgreSQL serving tables

#### Candidate Gold Models
1. `gold_article_activity_daily`
2. `gold_article_pageviews_daily`
3. `gold_attention_gap_daily`
4. `gold_clickstream_top_edges`
5. `gold_wiki_activity_summary`

#### Tasks
1. Define analytical grain for each table
2. Join edit activity with pageviews
3. Compute derived metrics such as:
   - edit count
   - unique editors
   - average size delta
   - pageviews
   - edits-per-1000-pageviews
   - attention gap
4. Load final tables into PostgreSQL
5. Create indexes for BI query efficiency

#### Outputs
- PostgreSQL DDL
- mart build scripts
- populated serving tables

#### Exit Criteria
- Power BI-ready tables exist in PostgreSQL
- table definitions are documented
- query performance is acceptable for demo usage

#### Questions answered by this phase
- Which articles are over-read but under-edited?
- Which articles have strong editorial activity relative to readership?
- Which clickstream edges dominate traffic into selected pages?

---

### Phase 7 — Power BI Reporting Layer
#### Goal
Expose project outputs through clear, interview-friendly dashboards.

#### Scope
- PostgreSQL connection
- report design
- dashboard narrative

#### Suggested Dashboard Pages
1. **Streaming Activity Overview**
   - edits by day/hour
   - top active wikis
   - bot vs non-bot edits

2. **Article Demand vs Editorial Attention**
   - pageviews vs edits
   - attention gap ranking
   - top edited and top viewed pages

3. **Clickstream Navigation**
   - top inbound pages
   - top outbound pages
   - selected article relationship view

4. **Trend / Event Monitoring**
   - pages with sudden changes
   - time-series trends for selected articles

#### Tasks
1. Connect Power BI to PostgreSQL
2. Import serving tables
3. Build semantic measures where useful
4. Design 2–4 clear report pages
5. Capture screenshots for repo documentation

#### Outputs
- Power BI dashboard file
- screenshots for GitHub README
- metric dictionary

#### Exit Criteria
- dashboards refresh from PostgreSQL
- charts tell a clear story
- visuals are clean enough for portfolio use

---

### Phase 8 — Hardening, Validation, and Documentation
#### Goal
Make the project reproducible and credible as a portfolio artifact.

#### Scope
- data checks
- developer experience
- documentation

#### Tasks
1. Add row-count and freshness checks
2. Add sanity checks on nulls / duplicates
3. Document service startup order
4. Write README:
   - architecture
   - stack
   - how to run
   - screenshots
5. Document known limitations
6. Add teardown / reset instructions

#### Outputs
- final README
- setup instructions
- troubleshooting notes
- example screenshots

#### Exit Criteria
- another person can understand the project
- repo is clean and structured
- project can be demoed reliably

---

## 10. Recommended Build Order

```text
1. Docker infra first
2. SSE producer -> Kafka
3. Spark streaming -> MinIO
4. Airflow pageviews DAG
5. Airflow clickstream DAG
6. Gold models -> PostgreSQL
7. Power BI dashboards
8. Documentation and polish
```

This order minimizes wasted effort because it proves the hardest integration points early.

---

## 11. Proposed Analytics Questions

These are the concrete “analyse this / analyse that” goals of the project.

### Streaming / editorial activity
- Which pages are receiving the most edits in the last hour/day?
- Which wiki editions are most active?
- What share of edits are made by bots?

### Reader demand
- Which pages have the most pageviews among the monitored set?
- Do high-pageview pages also have high edit activity?

### Editorial attention vs reader demand
- Which pages have **high pageviews but low edits**?
- Which pages have **high edits but low pageviews**?
- Which pages are “balanced” between readership and editorial activity?

### Navigation / clickstream
- Which pages are the main traffic referrers into selected pages?
- Which destinations do readers commonly visit next?

### Trend monitoring
- Which pages show simultaneous edit and pageview spikes?
- Which topics may reflect rapid public attention shifts?

---

## 12. Storage Design

### MinIO buckets
Suggested buckets:
- `wikidata-bronze`
- `wikidata-silver`
- `wikidata-gold` (optional if gold is mostly served via PostgreSQL)

### PostgreSQL schemas
Suggested schemas:
- `staging`
- `mart`
- `monitoring` (optional)

### Example object layout

```text
wikidata-bronze/
  recentchange/dt=2026-03-12/wiki=enwiki/part-*.parquet
  pageviews/dt=2026-03-12/article=Artificial_intelligence/*.json
  clickstream/month=2026-02/wiki=enwiki/*.tsv

wikidata-silver/
  edit_events/dt=2026-03-12/wiki=enwiki/*.parquet
  article_pageviews/dt=2026-03-12/*.parquet
  clickstream_edges/month=2026-02/wiki=enwiki/*.parquet
```

---

## 13. Risks and Mitigations

| Risk | Why it matters | Mitigation |
|---|---|---|
| Too many moving parts at once | project stalls early | build in strict phase order |
| Spark + Kafka integration issues | common configuration pain | validate Kafka flow before Spark |
| Power BI work expands too much | BI steals focus from DE core | keep dashboards simple |
| Pageviews API scope too broad | too many API calls | start with curated page list |
| Clickstream file size complexity | slower iteration | start with one language and one month |
| Over-modeling PostgreSQL | slows delivery | create only a few mart tables first |

---

## 14. Definition of Success

The POC is successful when:

- Docker Compose starts the full platform reliably
- live Wikimedia events flow into Kafka
- Spark writes cleaned event data to MinIO
- Airflow ingests pageviews and clickstream data on schedule
- curated marts are available in PostgreSQL
- Power BI dashboards can answer the project’s core analytics questions

---

## 15. Important References

### Core Data Sources
- Wikimedia EventStreams recentchange:  
  `https://stream.wikimedia.org/v2/stream/recentchange`

- Wikimedia Pageviews API reference:  
  `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article`

- Wikimedia Clickstream dump index:  
  `https://dumps.wikimedia.org/other/clickstream/`

### Core Documentation
- Apache Airflow docs:  
  `https://airflow.apache.org/docs/`

- Apache Spark docs:  
  `https://spark.apache.org/docs/latest/`

- Apache Kafka docs:  
  `https://kafka.apache.org/documentation/`

- Docker Compose docs:  
  `https://docs.docker.com/compose/`

- PostgreSQL docs:  
  `https://www.postgresql.org/docs/`

### Supporting Documentation
- MinIO container / object storage docs:  
  `https://min.io/docs/minio/container/index.html`

- Power BI PostgreSQL connectivity reference:  
  `https://learn.microsoft.com/power-bi/connect-data/desktop-connect-postgresql`

---

## 16. Suggested Next Step After This Plan

Start with **Phase 1 only** and do not code the full pipeline yet.

The first concrete milestone should be:

```text
docker compose up
```

with these services healthy:
- zookeeper
- kafka
- postgres
- minio
- airflow-webserver
- airflow-scheduler
- spark-master
- spark-worker

Only after that should you implement the SSE producer.

---

## 17. Final Notes

This design is intentionally:
- **real-data based**
- **portfolio credible**
- **complex enough to be impressive**
- but still **small enough to finish**

The strongest version of this project is not the most ambitious one.  
It is the one you can **complete, explain clearly, and demo confidently**.
