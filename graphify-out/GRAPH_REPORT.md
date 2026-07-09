# Graph Report - .  (2026-07-04)

## Corpus Check
- 19 files · ~88,855 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 71 nodes · 77 edges · 14 communities (7 shown, 7 thin omitted)
- Extraction: 77% EXTRACTED · 23% INFERRED · 0% AMBIGUOUS · INFERRED: 18 edges (avg confidence: 0.83)
- Token cost: 0 input · 94,623 output

## Community Hubs (Navigation)
- [[_COMMUNITY_KafkaDocker Infra Fixes|Kafka/Docker Infra Fixes]]
- [[_COMMUNITY_WikiPulse Docker-Only Plan (v1)|WikiPulse Docker-Only Plan (v1)]]
- [[_COMMUNITY_Live SSE Producer (prod)|Live SSE Producer (prod)]]
- [[_COMMUNITY_SSE Retry & Backfill Design|SSE Retry & Backfill Design]]
- [[_COMMUNITY_EventStreams Schema & Checkpoints|EventStreams Schema & Checkpoints]]
- [[_COMMUNITY_WikiPulse Revised Plan (v1.2 KRaft)|WikiPulse Revised Plan (v1.2 KRaft)]]
- [[_COMMUNITY_Project Goal & README|Project Goal & README]]
- [[_COMMUNITY_Kafka Commands Script|Kafka Commands Script]]
- [[_COMMUNITY_Kafka Debug Script|Kafka Debug Script]]
- [[_COMMUNITY_Branching Strategy|Branching Strategy]]
- [[_COMMUNITY_Clickstream Dumps Source|Clickstream Dumps Source]]
- [[_COMMUNITY_Project Phases|Project Phases]]
- [[_COMMUNITY_Repo Layout Convention|Repo Layout Convention]]

## God Nodes (most connected - your core abstractions)
1. `WikiPulse POC Execution Plan (v1)` - 8 edges
2. `WikiPulse Docker-Only Execution Plan (v1)` - 6 edges
3. `Two-regime SSE recovery model (core decision)` - 5 edges
4. `docker-compose kafka service (KRaft mode)` - 5 edges
5. `Outer reconnect loop design (retryable exceptions)` - 5 edges
6. `kafka_produce()` - 4 edges
7. `Stack table (Python, Docker, Kafka KRaft, Postgres, kafka-ui)` - 4 edges
8. `docker-compose python-sse service` - 4 edges
9. `WikiPulse Revised Execution Plan (v1.2, Docker-first KRaft)` - 4 edges
10. `Current baseline infrastructure (Postgres + Kafka KRaft up and validated)` - 4 edges

## Surprising Connections (you probably didn't know these)
- `Wikimedia EventStreams (SSE) data source` --semantically_similar_to--> `WikiPulse POC Execution Plan (v1)`  [INFERRED] [semantically similar]
  CLAUDE.md → docs/Execution_Plan.md
- `Wikimedia Pageviews dumps data source` --semantically_similar_to--> `Attention Gap model (pageviews / edits)`  [INFERRED] [semantically similar]
  CLAUDE.md → docs/Execution_Plan.md
- `Gap recovery two-regime model (target design)` --semantically_similar_to--> `Outer reconnect loop design (retryable exceptions)`  [INFERRED] [semantically similar]
  CLAUDE.md → docs/Kafka.md
- `Delivery Guarantees & Key Principles` --conceptually_related_to--> `Outer reconnect loop design (retryable exceptions)`  [INFERRED]
  CLAUDE.md → docs/Kafka.md
- `wikimedia.recentchange.deadletter quarantine topic` --semantically_similar_to--> `Kafka topic wikimedia.recentchange.backfill`  [INFERRED] [semantically similar]
  docs/Kafka.md → claude-sessions/2026-06-30-sse-retry-recovery-backfill-design.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WikiPulse execution plan version lineage (v1 -> Docker-only v1 -> v1.2 KRaft)** — docs_execution_plan_wikipulse, docs_execution_plan_2_wikipulse_docker_only, docs_execution_plan_3_revised_plan [INFERRED 0.85]
- **SSE ingestion resilience/retry design effort across sessions** — claude_md_gap_recovery_design, docs_kafka_reconnect_loop_design, claude_sessions_2026_06_30_sse_retry_recovery_backfill_design_two_regime_recovery_model [INFERRED 0.85]
- **Kafka dual-listener (internal/external) topology pattern** — docker_compose_kafka_service, docker_compose_kafka_ui_service, kafka_error_fix_advertised_listeners_solution [INFERRED 0.85]

## Communities (14 total, 7 thin omitted)

### Community 0 - "Kafka/Docker Infra Fixes"
Cohesion: 0.22
Nodes (13): Stack table (Python, Docker, Kafka KRaft, Postgres, kafka-ui), docker-compose kafka service (KRaft mode), docker-compose kafka-ui service, docker-compose postgres service, docker-compose python-sse service, Current baseline infrastructure (Postgres + Kafka KRaft up and validated), requests_sse.client.InvalidStatusCodeError (unhandled exception), Wikimedia SSE HTTP 500 incident (+5 more)

### Community 1 - "WikiPulse Docker-Only Plan (v1)"
Cohesion: 0.20
Nodes (11): Container plan (zookeeper, kafka, spark, postgres, airflow, minio, producer), Execution Phases 0-8 (infra to hardening), Bronze/Silver/Gold logical layers (Docker-only plan), Power BI reporting layer, WikiPulse Docker-Only Execution Plan (v1), Airflow DAGs (streaming monitor, pageview, clickstream, analytics build), Kafka topic design (recentchange_raw/clean, article_activity), Kubernetes deployment plan (+3 more)

### Community 2 - "Live SSE Producer (prod)"
Cohesion: 0.29
Nodes (7): Architecture (current to target pipeline), Code Style conventions (tabs, no classes, print logging), create_producer(), delivery_report(), kafka_produce(), main(), Producer

### Community 3 - "SSE Retry & Backfill Design"
Cohesion: 0.22
Nodes (10): Gap recovery two-regime model (target design), Session Log table, backfill.py (planned separate backfill script), SSE Last-Event-ID resumption (short/medium gaps), Kafka topic wikimedia.recentchange.backfill, Kafka topic wikimedia.recentchange.raw, Two-regime SSE recovery model (core decision), wikimedia.recentchange.deadletter quarantine topic (+2 more)

### Community 4 - "EventStreams Schema & Checkpoints"
Cohesion: 0.29
Nodes (7): Delivery Guarantees & Key Principles, Wikimedia EventStreams (SSE) data source, backfill_queue.json gap list file, Checkpoint written in Kafka delivery callback (not on SSE read), resume_state.json checkpoint file, meta object (dt, stream, id, offset, partition, request_id), mediawiki/recentchange JSONSchema

### Community 5 - "WikiPulse Revised Plan (v1.2 KRaft)"
Cohesion: 0.29
Nodes (7): Wikimedia Pageviews dumps data source, Attention gap metric (edits-per-1000-pageviews), gold_attention_gap_daily model, Kafka KRaft mode decision (no ZooKeeper), Revised Execution Phases 0-11, WikiPulse Revised Execution Plan (v1.2, Docker-first KRaft), Attention Gap model (pageviews / edits)

## Knowledge Gaps
- **22 isolated node(s):** `commands.sh script`, `debug.sh script`, `Project Goal (Wikimedia clickstream/traffic analytics)`, `Wikimedia Pageviews dumps data source`, `Wikimedia monthly clickstream TSV dumps data source` (+17 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Current baseline infrastructure (Postgres + Kafka KRaft up and validated)` connect `Kafka/Docker Infra Fixes` to `SSE Retry & Backfill Design`, `WikiPulse Revised Plan (v1.2 KRaft)`?**
  _High betweenness centrality (0.289) - this node is a cross-community bridge._
- **Why does `Gap recovery two-regime model (target design)` connect `SSE Retry & Backfill Design` to `Kafka/Docker Infra Fixes`?**
  _High betweenness centrality (0.277) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `Outer reconnect loop design (retryable exceptions)` (e.g. with `Delivery Guarantees & Key Principles` and `Gap recovery two-regime model (target design)`) actually correct?**
  _`Outer reconnect loop design (retryable exceptions)` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `commands.sh script`, `debug.sh script`, `Project Goal (Wikimedia clickstream/traffic analytics)` to the rest of the system?**
  _25 weakly-connected nodes found - possible documentation gaps or missing edges._