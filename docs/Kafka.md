# Kafka Notes

## Wikimedia SSE HTTP 500 Incident

Source note: `codex-sessions/2026-06-18-wikimedia-sse-500.md`

### What happened

The producer did not fail because Kafka stopped accepting messages. The log excerpt shows the producer successfully delivered events to Kafka topic `wikimedia.recentchange.raw` up to partition `0`, offset `703782`.

The failure happened on the Wikimedia Server-Sent Events side while the producer was iterating over:

- `https://stream.wikimedia.org/v2/stream/recentchange`
- `event-stream/src/producer.py`, inside `for event in stream:`

The traceback ended with:

```text
requests_sse.client.InvalidStatusCodeError: fetch https://stream.wikimedia.org/v2/stream/recentchange failed with wrong response status: 500
```

That means the HTTP request received an HTTP response from the Wikimedia EventStreams endpoint, but the response status was `500`. A `500` is a server-side error response from the upstream service. It is not the same thing as a client-side connection timeout. A timeout usually shows up as a timeout exception from `requests` or the underlying socket layer, not as an HTTP status-code exception.

The current producer catches only:

- `KeyboardInterrupt`
- `RuntimeError`
- `TypeError`

It does not catch `requests_sse.client.InvalidStatusCodeError`, so the exception escapes the main loop. The `finally` block still runs, which is why the log shows shutdown and Kafka flush messages before the traceback is printed. After that, the Python process exits and Docker does not reconnect the stream unless the container is restarted by an external restart policy.

### Diagnosis

The incident should be treated as a retryable upstream SSE fetch failure unless more evidence points elsewhere.

Most likely sequence:

1. The producer was running normally and delivering messages to Kafka.
2. `requests_sse.EventSource` attempted to read from or reconnect to the Wikimedia `recentchange` stream.
3. Wikimedia returned HTTP `500`.
4. `requests_sse` raised `InvalidStatusCodeError`.
5. The producer did not handle that exception type.
6. The `finally` block flushed Kafka producer buffers.
7. The process exited permanently.

The important distinction is that the pipeline failure was caused by missing resiliency around the upstream SSE connection, not by a confirmed Kafka broker failure.

## Making `event-stream/src/producer.py` More Fail-Safe

The current script is acceptable for a first local proof of concept, but a UAT or production-like pipeline should expect transient upstream failures, network disconnects, Kafka backpressure, malformed events, process restarts, and duplicate data.

### 1. Separate responsibilities into functions

Refactor the script so the main flow is easier to retry and test.

Suggested function boundaries:

- `build_kafka_producer()`: creates the `confluent_kafka.Producer` from environment-driven config.
- `create_event_source(last_event_id=None)`: creates the Wikimedia `EventSource`.
- `parse_event(event)`: ignores non-message events, parses JSON, skips canary events, and validates required fields.
- `build_kafka_key(change)`: returns a stable key for partitioning and deduplication.
- `produce_change(producer, change)`: serializes and queues one Kafka message.
- `run_stream(producer)`: owns the SSE read loop.
- `run_forever()`: owns retry, backoff, shutdown, and final flush.

This lets the retry loop wrap the whole SSE connection lifecycle instead of wrapping only one event.

### 2. Add an outer reconnect loop

The producer should treat upstream HTTP `500`, dropped connections, read timeouts, and temporary DNS/network failures as retryable.

High-level flow:

1. Start producer once.
2. Enter `while not shutdown`.
3. Open a fresh `EventSource`.
4. Process events until shutdown or exception.
5. On retryable exception, log the error class, status/detail, retry count, and wait time.
6. Sleep with exponential backoff plus jitter.
7. Reconnect.
8. On clean shutdown, flush Kafka and exit.

Do not let one bad upstream response permanently terminate the container.

Useful exception groups to consider:

- `requests_sse.client.InvalidStatusCodeError`
- `requests.exceptions.Timeout`
- `requests.exceptions.ConnectionError`
- `requests.exceptions.ChunkedEncodingError`
- selected generic `requests.exceptions.RequestException`

Keep `KeyboardInterrupt` and `SIGTERM` behavior separate so local Ctrl-C and Docker stop still exit promptly.

### 3. Use bounded exponential backoff with jitter

Avoid reconnecting in a tight loop during an upstream outage.

Suggested behavior:

- start around `1` to `5` seconds
- double after each consecutive failure
- cap around `60` seconds for UAT
- add small random jitter so multiple replicas do not reconnect at the same instant
- reset the backoff after a stable connection or after a successful event batch

Expose the values as environment variables later if the script is promoted beyond local development.

### 4. Track recovery position and data-loss boundaries

Kafka can only protect events after they have been produced to Kafka. If the producer is down or disconnected before reading an event from Wikimedia, Kafka has no copy of that missed event.

For recovery, track the latest successfully processed upstream event identity. Candidate fields:

- SSE event ID, if exposed by the client and supported by Wikimedia EventStreams reconnection.
- `change["meta"]["id"]`, if present and stable.
- `change["meta"]["dt"]` for coarse progress logging.
- recentchange-specific IDs such as `rcid`, when present.

Store progress only after Kafka delivery succeeds, not merely after reading the SSE event. The delivery callback is the safest place to confirm that Kafka accepted the message.

Recovery options:

- Best case: reconnect using an upstream-supported resume mechanism such as `Last-Event-ID`, after verifying current Wikimedia EventStreams behavior and the `requests_sse` API.
- Practical UAT case: accept that live-stream gaps can happen, but make them observable with reconnect logs, last seen event IDs, and gap counters.
- Stronger pipeline case: add a periodic reconciliation job using a batch or API source to backfill recent changes by timestamp or ID.

Do not claim exactly-once delivery from this producer alone. A realistic target is at-least-once ingestion with downstream deduplication.

### 5. Make Kafka publishing safer

Recommended producer configuration areas:

- `enable.idempotence=true` to reduce duplicate writes caused by Kafka retries.
- `acks=all` so Kafka only acknowledges after the configured replicas accept the write.
- `retries` and `retry.backoff.ms` for broker-side transient errors.
- `delivery.timeout.ms` to avoid messages staying pending forever.
- `linger.ms` and `batch.size` only after measuring throughput needs.

Use a stable Kafka message key so duplicates and updates can be grouped consistently. Good key candidates for `recentchange` are usually based on stable event fields such as wiki/domain plus recentchange ID or event ID.

Also handle local producer backpressure:

- `producer.produce()` can fail when the local queue is full.
- Poll regularly to drain delivery callbacks.
- If the queue is full, poll and retry briefly instead of dropping immediately.
- If the message still cannot be queued, log it clearly and consider a dead-letter path.

### 6. Add a dead-letter or quarantine path

Malformed events should not crash the stream, but they should not disappear silently.

For UAT, logging parse failures may be enough.

For a stronger pipeline, write failed records to a separate Kafka topic such as:

- `wikimedia.recentchange.deadletter`

Include:

- failure reason
- original raw payload, when available
- timestamp
- producer instance ID
- upstream event ID, if available

### 7. Improve observability

The current producer prints useful delivery logs, but production-like debugging needs lifecycle and health signals too.

Add structured log fields for:

- stream start
- stream stop
- reconnect attempt number
- exception type
- HTTP status code, when available
- backoff seconds
- last seen upstream event ID
- last delivered Kafka topic, partition, and offset
- count of parsed, skipped, produced, delivered, failed, and dead-lettered events

For UAT, plain JSON logs to stdout are enough because Docker can collect them. Later, Prometheus metrics or OpenTelemetry can be added.

### 8. Preserve graceful shutdown

The signal handler should set a shutdown flag only. The main loop should notice the flag, stop reconnecting, and flush Kafka once.

Expected shutdown behavior:

1. `SIGTERM` or Ctrl-C sets `shutdown=True`.
2. The current stream loop exits.
3. No reconnect is attempted.
4. Kafka producer flushes with a bounded timeout.
5. The process exits with a clear final log message.

Avoid swallowing all exceptions with a broad `except Exception` unless the log includes the exception type and the code still distinguishes retryable errors from programmer errors.

### 9. Add container-level safety

Application retry is the first line of defense, but Docker can provide a second line.

Consider adding a restart policy for the producer service:

- `restart: unless-stopped` for local/UAT
- or `restart: on-failure` if manual stops should stay stopped

This does not replace application-level reconnect logic. It only helps if the process crashes unexpectedly.

### 10. Validation checklist

After refactoring, validate with Docker Compose:

1. Start Kafka, Kafka UI, and the producer.
2. Confirm messages are delivered to `wikimedia.recentchange.raw`.
3. Stop network access or point the stream URL to a bad endpoint temporarily.
4. Confirm the producer logs retryable failures and reconnect attempts.
5. Restore the valid URL.
6. Confirm the producer resumes without a container restart.
7. Stop the container with Docker.
8. Confirm the producer flushes and exits cleanly.
9. Check Kafka topic offsets before and after the test.
10. Review logs for duplicate, skipped, failed, and dead-lettered event counts.

## Summary

The logged HTTP `500` was an upstream Wikimedia EventStreams response, not strong evidence of a local connection timeout. The producer exited because that retryable SSE exception was outside the current `except` clause. The main hardening work is to wrap the SSE connection in a retry/reconnect loop, track upstream progress, publish to Kafka with stronger delivery settings, make data-loss limits explicit, and add observability around reconnects and delivery outcomes.
