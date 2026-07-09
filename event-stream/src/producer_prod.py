import json
import os
import socket
import sys
from requests_sse import EventSource
from confluent_kafka import Producer
import datetime
import signal
import time
import traceback

# Kafka broker
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# EventStream
EVENTSTREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
EVENTSTREAM_HEADER = {"User-Agent": "Wikimedia-Analytics/0.1 nacht29.study@gmail.com"}
KAFKA_STREAM = os.getenv("KAFKA_STREAM", "wikimedia.recentchange.raw")
KAFKA_BACKFILL = os.getenv("KAFKA_BACKFILL", "wikimedia.recentchange.backfill")

# Retries
FAILED_RETRY = int(os.getenv("FAILED_RETRY", "5"))
RETRY_WAIT = float(os.getenv("RETRY_WAIT", "1"))
MAX_RETRY_WAIT = float(os.getenv("MAX_RETRY_WAIT", "5")) # starting SSE stream
EVENTSTREAM_TIMEOUT = float(os.getenv("EVENTSTREAM_TIMEOUT", "5")) # change to 15 later
EVENTSTREAM_RETRY_WAIT = float(os.getenv("EVENTSTREAM_RETRY_WAIT", "1"))
EVENTSTREAM_CONNECT_RETRY = int(os.getenv("EVENTSTREAM_CONNECT_RETRY", "1"))
KAFKA_FLUSH_TIMEOUT = float(os.getenv("KAFKA_FLUSH_TIMEOUT", "10"))

# shutdown handler
shutdown = False
current_stream = None

def handle_shutdown(signum:int, frame):
	global shutdown, current_stream
	shutdown = True
	print(f"{datetime.datetime.now()}\t|\tShutdown requested.", flush = True)
	if current_stream:
		current_stream.close()

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def delivery_report(error:Exception, message:str, event_id:str):
	if error:
		error_code = error.code() if hasattr(error, "code") else None
		error_name = error.name() if hasattr(error, "name") else None
		print(
			f"{datetime.datetime.now()}\t|\tFailed to deliver message to Kafka topic: {message.topic()}. Event ID: {event_id}"
			f"code={error_code} name={error_name} detail={error}",
			flush=True
		)
	else:
		print(
			f"{datetime.datetime.now()}\t|\tDelivered 1 message to Kafka topic: {message.topic()}. Event ID: {event_id}"
			f"partition={message.partition()} offset={message.offset()}",
			flush=True
		)

def log(msg:str):
	msg = msg.split('\n')
	for line in msg:
		print(f"{datetime.datetime.now()}\t|\t{line}", flush=True)

def create_producer() -> Producer:
	# Kafka bootstrap server config
	config = {
		"bootstrap.servers": BOOTSTRAP_SERVERS,
		"client.id": socket.gethostname(),
		"acks": "all",
		"enable.idempotence": True,
		"retries": 5,
		"retry.backoff.ms": 1000,
		"reconnect.backoff.max.ms": 10000,
		"request.timeout.ms": 15000,
		"socket.timeout.ms": 15000,
		"message.timeout.ms": 30000
	}
	return Producer(config) # create Kafka producer

def kafka_produce(producer:Producer):
	global shutdown, current_stream

	point_retry = 0
	hard_retry_limit = 0
	last_id = None

	while point_retry < 5:
		if hard_retry_limit == 5:
			sys.exit
		try:
			with EventSource(
				url=EVENTSTREAM_URL,
				latest_event_id=last_id,
				headers=EVENTSTREAM_HEADER,
				timeout=EVENTSTREAM_TIMEOUT,
				reconnection_time=datetime.timedelta(seconds=EVENTSTREAM_RETRY_WAIT),
				max_connect_retry=EVENTSTREAM_CONNECT_RETRY
			) as stream:
				current_stream = stream # used close EventSource stream upon requested shutdown
				log("SSE Started")
				for event in stream:
					if shutdown:
						break
					if event.type != 'message':
						continue
					try:
						change = json.loads(event.data) # load stream data as dict
					except ValueError:
						pass
					except Exception as error: # json.JSONDecodeError
						log(f"Failed to load data from: {EVENTSTREAM_URL}")
						log(f"Error: {error}")
						point_retry += 1
						last_id = event.last_event_id
						log(f"Soft limit: {point_retry}")
						log(f"Hard limit: {hard_retry_limit}")
					else:
						if point_retry == 5:
							hard_retry_limit += 1
							break
						if change['meta']['domain'] == 'canary':
							continue
					# create a broker instance and write to topic
					value = json.dumps(change)
					producer.produce( # queue message
						topic=KAFKA_STREAM,
						# key = key,
						value=value,
						callback=lambda error, message: delivery_report(error, message, event_id=change["id"]) # change: dict | value: JSON dumps (str) -> use change to retrive event.id
					)
					producer.poll(0) # checks Kafka producer event, drains event queue and execute callback based on the message received upon wrtiting to Kafka
		finally:
			current_stream = None
			print(f"{datetime.datetime.now()}\t|\tFlushing messages...", flush = True)
			remaining_messages = producer.flush(KAFKA_FLUSH_TIMEOUT) # flush producer before closing - Kafa batches message before sending and rerturn numbers of messages unflushed
			if remaining_messages: # if there are messages unflushed - return value from flush()
				print(f"{datetime.datetime.now()}\t|\tFailed to flush {remaining_messages} message(s).", flush = True)
			print(f"{datetime.datetime.now()}\t|\tFlush complete.", flush = True)
		
		point_retry = 0

def main():
	retry = 0
	wait = RETRY_WAIT

	def handle_retry(err_msg:str, traceback_msg:str):
		nonlocal retry, wait
		retry += 1
		wait = min(wait * 2, MAX_RETRY_WAIT)
		log(err_msg)
		log(traceback_msg)
		if retry < FAILED_RETRY:
			time.sleep(wait)

	while not shutdown and retry < FAILED_RETRY:
		try:
			producer = create_producer()
		except Exception:
			handle_retry(
				err_msg="Failed to start Kafka producer.",
				traceback_msg=traceback.format_exc()
			)
			continue

		try:
			kafka_produce(producer)
		except Exception:
			if shutdown:
				break
			handle_retry(
				err_msg="Failed to start SSE stream.",
				traceback_msg=traceback.format_exc()
			)
			continue

if __name__ == '__main__':
	main()
