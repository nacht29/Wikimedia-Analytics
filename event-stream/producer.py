import json
from requests_sse import EventSource
from confluent_kafka import Producer
import socket

# Kafka broker
BOOTSTRAP_SERVERS = "localhost:29092"

# EventStream
EVENTSTREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
EVENTSTREAM_HEADER = {"User-Agent": "Wikimedia-Analytics/0.1 nacht29.study@gmail.com"}
KAFKA_TOPIC = "wikimedia.test.raw"

# Kafka bootstrap server config
config = {
	"bootstrap.servers": BOOTSTRAP_SERVERS,
	"client.id": socket.gethostname()
}

with EventSource(url=EVENTSTREAM_URL, headers=EVENTSTREAM_HEADER) as stream:
	key = 0
	for event in stream:
		if event.type == 'message':
			try:
				change = json.loads(event.data)
			except ValueError as error:
				print(
					f"Failed to load data: {EVENTSTREAM_URL}"
					f"Error: {error}"
				)
			else:
				if change['meta']['domain'] == 'canary':
					continue

				# create a broker instance and write to topic
				with Producer(config) as producer:
					value = f"{change['user']} edited {change['title']}"
					producer.produce(topic=KAFKA_TOPIC, value=value)
					producer.poll(0)
					producer.flush()
				key += 1

'''
from pywikibot.comms.eventstreams import EventStreams
stream = EventStreams(streams=['recentchange', 'revision-create'], since='20250107')
stream.register_filter(server_name='fr.wikipedia.org', type='edit')
change = next(stream)
print('{type} on page "{title}" by "{user}" at {meta[dt]}.'.format(**change))
'''
