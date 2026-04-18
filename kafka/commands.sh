# show topics
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# inspect producer container
docker inspect wikimedia-analytics-python-sse-1 --format '{{json .Config.Env}}'
