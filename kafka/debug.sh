#!/usr/bin/env bash
set -euo pipefail

# Kafka UI / Kafka connectivity debugging commands used for this incident.
# Note: Docker commands require access to the Docker daemon.

sed -n '1,240p' 'kafka-ui.log'
sed -n '1,260p' 'docker-compose.yml'
rg -n "kafka-ui|provectuslabs/kafka-ui|KAFKA_CLUSTERS|BOOTSTRAP" -S .
rg -n "localhost:9092|kafka:9092|29092|KAFKA_ADVERTISED_LISTENERS|BOOTSTRAP_SERVERS|bootstrap.servers" -S .
nl -ba 'docker-compose.yml'
nl -ba 'kafka-ui.log' | sed -n '15,70p'
nl -ba 'kafka-ui.log' | sed -n '71,95p'
nl -ba 'kafka-ui.log' | sed -n '95,103p'
docker compose config
docker compose up -d kafka kafka-ui
docker compose ps
docker compose logs --tail=120 kafka-ui
docker compose logs --tail=120 kafka
docker compose logs kafka | rg -n "advertised.listeners|listeners =|inter.broker.listener.name|controller.quorum.voters" -n
docker compose restart kafka-ui
sleep 8
docker compose logs --tail=120 kafka-ui
docker compose logs --since=90s kafka-ui | rg -n "localhost/127.0.0.1:9092|bootstrap.servers|Metrics updated|Started KafkaUiApplication|Timed out waiting for a node assignment"
docker compose logs --since=90s kafka-ui | tail -n 80
