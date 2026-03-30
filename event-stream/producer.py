import json
import socket
from requests_sse import EventSource
from confluent_kafka import Producer
import datetime
import signal

# Kafka broker
BOOTSTRAP_SERVERS = "localhost:29092"

# EventStream
EVENTSTREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
EVENTSTREAM_HEADER = {"User-Agent": "Wikimedia-Analytics/0.1 nacht29.study@gmail.com"}
KAFKA_TOPIC = "wikimedia.test_recentchange.raw"

# shutdown handler
shutdown = False

def handle_shutdown(signum, frame):
	global shutdown
	shutdown = True
	print(f"{datetime.datetime.now()}	|	Shutdown requested.")

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Kafka bootstrap server config
config = {
	"bootstrap.servers": BOOTSTRAP_SERVERS,
	"client.id": socket.gethostname()
}

# create Kafka producer
producer = Producer(config)
try:
	with EventSource(url=EVENTSTREAM_URL, headers=EVENTSTREAM_HEADER) as stream:
		print(f"{datetime.datetime.now()}	|	SSE Started")
		for event in stream:
			if shutdown == True:
				break
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
			value = json.dumps(change, indent=4)
			producer.produce( # queue message
				topic=KAFKA_TOPIC,
				# key = key,
				value=value
			)
			print(f"{datetime.datetime.now()}	|	Loaded 1 message to {KAFKA_TOPIC}")
			producer.poll(0) # drain queue amd execute callback - check for events but don't block; process completed deliveries and return instantly
except (KeyboardInterrupt, RuntimeError, TypeError):
	pass
finally:
	print(f"{datetime.datetime.now()}	|	Shutdown in progress. Flushing messages...")
	producer.flush() # flush producer before closing - Kafa batches message before sending
	print(f"{datetime.datetime.now()}	|	Shutdown complete.")

'''
from pywikibot.comms.eventstreams import EventStreams
stream = EventStreams(streams=['recentchange', 'revision-create'], since='20250107')
stream.register_filter(server_name='fr.wikipedia.org', type='edit')
change = next(stream)
print('{type} on page "{title}" by "{user}" at {meta[dt]}.'.format(**change))
'''
