# WikiPulse

### Real-Time Wikimedia Editorial & Attention Analytics Platform

**Proof-of-Concept (POC) Execution Plan**

------------------------------------------------------------------------

# 1. Project Overview

## 1.1 Objective

Build an **end-to-end real-time data engineering platform** that ingests
**live Wikimedia edit activity**, processes it through **streaming and
batch pipelines**, and produces analytics about:

-   editorial activity
-   reader demand
-   knowledge propagation across Wikipedia

The system demonstrates the following **mid-level data engineering
capabilities**:

-   streaming ingestion
-   distributed processing
-   orchestration
-   containerized infrastructure
-   Kubernetes deployment
-   hybrid streaming + batch architecture

------------------------------------------------------------------------

# 2. Learning Objectives

This project is designed to deepen understanding of:

  Domain                Skills
  --------------------- ----------------------------
  Streaming systems     Kafka event pipelines
  Distributed compute   Spark Structured Streaming
  Data orchestration    Airflow DAG scheduling
  Infrastructure        Docker + Kubernetes
  Data architecture     Medallion architecture
  Data modeling         analytics aggregates
  Observability         pipeline monitoring

------------------------------------------------------------------------

# 3. Technology Stack

## Core Tools

  Layer                   Technology
  ----------------------- -----------------------
  Streaming ingestion     Apache Kafka
  Processing              Apache Spark
  Orchestration           Apache Airflow
  Containerization        Docker
  Cluster orchestration   Kubernetes
  Storage                 MinIO (S3-compatible)
  Analytics engine        DuckDB / Postgres
  Visualization           Apache Superset

------------------------------------------------------------------------

# 4. Data Sources

## 4.1 Wikimedia EventStreams (Real-Time)

Primary streaming source.

	https://stream.wikimedia.org/v2/stream/recentchange

Example event:

``` json
{
 "type": "edit",
 "title": "Artificial intelligence",
 "timestamp": 1700000000,
 "user": "ExampleUser",
 "wiki": "enwiki",
 "comment": "Updated section",
 "bot": false,
 "length": {
   "new": 20000,
   "old": 19800
 }
}
```

------------------------------------------------------------------------

## 4.2 Wikimedia Pageview API

Batch enrichment dataset.

Example endpoint:

	https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article

Used to measure:

-   reader demand
-   article popularity

------------------------------------------------------------------------

## 4.3 Clickstream Dataset

Monthly dataset containing:

	(referrer_page → destination_page)

Used for:

-   knowledge graph relationships
-   traffic flow between articles

------------------------------------------------------------------------

# 5. System Architecture

## 5.1 High-Level Architecture

					+----------------------+
					| Wikimedia EventStream|
					|  (recentchange SSE)  |
					+-----------+----------+
								|
								v
						 +--------------+
						 | Ingestion    |
						 | Python SSE   |
						 | Producer     |
						 +------+-------+
								|
								v
						   +---------+
						   | Kafka   |
						   | Topics  |
						   +----+----+
								|
								v
				  +----------------------------+
				  | Spark Structured Streaming |
				  |                            |
				  | Parse + Normalize          |
				  | Basic Aggregations         |
				  +-------------+--------------+
								|
								v
						+----------------+
						| Data Lake      |
						| MinIO / S3     |
						| Parquet tables |
						+--------+-------+
								 |
								 v
						+----------------+
						| Spark Batch    |
						| Enrichment     |
						+--------+-------+
								 |
								 v
						  +--------------+
						  | Warehouse    |
						  | DuckDB       |
						  +------+-------+
								 |
								 v
						   +----------+
						   | Superset |
						   +----------+

------------------------------------------------------------------------

# 6. Logical Data Architecture

				RAW STREAM
				   |
				   v
			  Bronze Layer
			  raw event logs
				   |
				   v
			  Silver Layer
		   cleaned normalized data
				   |
				   v
			  Gold Layer
		   analytics aggregates

------------------------------------------------------------------------

# 7. Infrastructure Architecture

## Docker Development Environment

			 +--------------------+
			 | Docker Compose     |
			 +---------+----------+

	   +------------+     +-------------+
	   | Zookeeper  |     | Kafka       |
	   +------------+     +-------------+

	   +------------+     +-------------+
	   | Spark      |     | Airflow     |
	   +------------+     +-------------+

	   +------------+     +-------------+
	   | MinIO      |     | Superset    |
	   +------------+     +-------------+

	   +-------------------------------+
	   | Python Event Producer         |
	   +-------------------------------+

------------------------------------------------------------------------

## Kubernetes Deployment

				+----------------------+
				| Kubernetes Cluster   |
				+----------+-----------+

		 +------------+			+------------+
		 | Kafka Pod  |			| Kafka Pod  |
		 +------------+			+------------+

			+-----------------------------+
			| Spark Streaming Pods        |
			+-----------------------------+

		  +-------------+    +-------------+
		  | Airflow     |    | Airflow     |
		  | Scheduler   |    | Workers     |
		  +-------------+    +-------------+

		  +-------------+    +-------------+
		  | MinIO       |    | Superset    |
		  +-------------+    +-------------+

------------------------------------------------------------------------

# 8. Kafka Design

  Topic                Purpose
  -------------------- ----------------------
  recentchange_raw     raw Wikimedia events
  recentchange_clean   normalized events
  article_activity     aggregated events

Partition key:

	wiki + article_title

------------------------------------------------------------------------

# 9. Spark Streaming Pipeline

	Kafka Topic
		 |
		 v
	Spark Structured Streaming
		 |
		 v
	JSON parsing
		 |
		 v
	Schema normalization
		 |
		 v
	Write to Bronze Parquet

Example streaming metrics:

-   edits_per_article_per_minute
-   top_editors
-   edit_size_changes
-   edit_spike_detection

------------------------------------------------------------------------

# 10. Batch Enrichment Pipeline

	Download pageview metrics
			|
			v
	Fetch clickstream dataset
			|
			v
	Join with article activity
			|
			v
	Produce analytics tables

------------------------------------------------------------------------

# 11. Airflow DAGs

## DAG 1 --- Streaming Monitor

	check_kafka_health
		↓
	check_spark_stream
		↓
	alert_if_failed

## DAG 2 --- Pageview Enrichment

	fetch_pageview_data
		  ↓
	clean_pageview_data
		  ↓
	store_to_datalake

## DAG 3 --- Clickstream Processing

	download_clickstream
		  ↓
	extract_dataset
		  ↓
	build_click_graph

## DAG 4 --- Analytics Build

	load_bronze
	   ↓
	build_silver
	   ↓
	build_gold
	   ↓
	refresh_dashboard

------------------------------------------------------------------------

# 12. Storage Layout

	/data-lake

	/bronze
		/recentchange

	/silver
		/articles
		/edit_events

	/gold
		/article_activity
		/attention_gap
		/clickstream_graph

Format:

	Parquet

------------------------------------------------------------------------

# 13. Analytics Models

## Article Activity

	article_activity_hourly

  column           description
  ---------------- -----------------------
  article          article title
  wiki             language edition
  edit_count       number of edits
  unique_editors   editors
  avg_edit_size    average revision size

------------------------------------------------------------------------

## Attention Gap Model

	attention_gap = pageviews / edits

Large values indicate high readership but low editing activity.

------------------------------------------------------------------------

# 14. Dashboards

Visualizations:

-   top edited pages
-   reader demand trends
-   attention gap ranking
-   clickstream network graph

------------------------------------------------------------------------

# 15. Development Phases

### Phase 1 --- Infrastructure Setup

-   Docker Compose stack
-   Kafka cluster
-   Spark cluster
-   Airflow instance
-   MinIO storage

### Phase 2 --- Streaming Ingestion

-   SSE client
-   publish events to Kafka
-   validate event schema

### Phase 3 --- Spark Streaming

-   build streaming consumer
-   parse events
-   write Parquet data

### Phase 4 --- Batch Pipelines

-   pageview ingestion
-   clickstream ingestion
-   Spark batch jobs

### Phase 5 --- Analytics

-   gold layer models
-   data quality checks

### Phase 6 --- Visualization

-   Superset dashboards

### Phase 7 --- Kubernetes Deployment

-   container images
-   deploy services
-   configure scaling

------------------------------------------------------------------------

# 16. Data Volume

Approximate rates:

	2–10 events/sec

Daily:

	200k – 800k events

Monthly with clickstream:

	>10GB

------------------------------------------------------------------------

# 17. Repository Structure

	wiki-pulse

	infra/
	  docker-compose.yml
	  k8s/

	airflow/
	  dags/

	spark/
	  streaming/
	  batch/

	kafka/
	  topics/

	ingestion/
	  wikimedia_producer/

	schemas/

	docs/
	  architecture.md
	  design.md

	dashboards/

------------------------------------------------------------------------

# 18. Success Criteria

Project succeeds when:

-   real-time Wikimedia events are ingested continuously
-   Spark processes streaming data reliably
-   Airflow orchestrates enrichment pipelines
-   analytics tables refresh automatically
-   dashboards display meaningful insights

------------------------------------------------------------------------

# 19. Portfolio Value

This project demonstrates:

-   real-time streaming pipelines
-   distributed compute with Spark
-   workflow orchestration
-   containerized infrastructure
-   Kubernetes deployment
-   hybrid batch + streaming architecture

These capabilities align with expectations for **mid-level data
engineers**.

------------------------------------------------------------------------

# 20. Future Extensions

Potential improvements:

-   anomaly detection with ML
-   topic clustering for articles
-   edit sentiment analysis
-   knowledge graph analytics
-   LLM summarization of edit trends

------------------------------------------------------------------------

# End of Document
