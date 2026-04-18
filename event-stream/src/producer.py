import json
import os
import socket
from requests_sse import EventSource
from confluent_kafka import Producer
import datetime
import signal

# Kafka broker
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# EventStream
EVENTSTREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
EVENTSTREAM_HEADER = {"User-Agent": "Wikimedia-Analytics/0.1 nacht29.study@gmail.com"}
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia.recentchange.raw")

# shutdown handler
shutdown = False

def handle_shutdown(signum:int, frame):
	global shutdown
	shutdown = True
	print(f"{datetime.datetime.now()}	|	Shutdown requested.", flush = True)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Kafka bootstrap server config
config = {
	"bootstrap.servers": BOOTSTRAP_SERVERS,
	"client.id": socket.gethostname()
}

# create Kafka producer
producer = Producer(config)

def delivery_report(error, message):
	if error:
		error_code = error.code() if hasattr(error, "code") else None
		error_name = error.name() if hasattr(error, "name") else None
		print(
			f"{datetime.datetime.now()}	|	Failed to deliver message to {message.topic()}: "
			f"code={error_code} name={error_name} detail={error}",
			flush=True
		)
	else:
		print(
			f"{datetime.datetime.now()}	|	Loaded 1 message to {message.topic()} "
			f"partition={message.partition()} offset={message.offset()}",
			flush=True
		)

try:
	with EventSource(url=EVENTSTREAM_URL, headers=EVENTSTREAM_HEADER) as stream:
		print(f"{datetime.datetime.now()}	|	SSE Started", flush = True)
		for event in stream:
			if shutdown == True:
				break
			if event.type != 'message':
				continue
			try:
				change = json.loads(event.data)
			except ValueError as error:
				print(
					f"Failed to load data: {EVENTSTREAM_URL}"
					f"Error: {error}",
					flush=True
				)
				continue
			if change['meta']['domain'] == 'canary':
				continue

			# create a broker instance and write to topic
			value = json.dumps(change)
			producer.produce( # queue message
				topic=KAFKA_TOPIC,
				# key = key,
				value=value,
				callback=delivery_report
			)
			producer.poll(0) # checks Kafka producer event, drains event queue and execute callback based on the message received upon wrtiting to Kafka
except (KeyboardInterrupt, RuntimeError, TypeError):
	pass
finally:
	print(f"{datetime.datetime.now()}	|	Shutdown in progress. Flushing messages...", flush = True)
	remaining_messages = producer.flush() # flush producer before closing - Kafa batches message before sending and rerturn numbers of messages unflushed
	if remaining_messages: # if there are messages unflushed - return value from flush()
		print(f"{datetime.datetime.now()}	|	Failed to flush {remaining_messages} message(s).", flush = True)
	print(f"{datetime.datetime.now()}	|	Shutdown complete.", flush = True)

'''
from pywikibot.comms.eventstreams import EventStreams
stream = EventStreams(streams=['recentchange', 'revision-create'], since='20250107')
stream.register_filter(server_name='fr.wikipedia.org', type='edit')
change = next(stream)
print('{type} on page "{title}" by "{user}" at {meta[dt]}.'.format(**change))
'''
