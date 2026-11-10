"""Kafka producer: validates each event against the contract, keys valid ones by vehicle_id,
and routes violations to a dead-letter topic with the reason in a header."""

import json
from collections import Counter
from collections.abc import Iterable

from confluent_kafka import KafkaException, Producer

from streamhouse import contract

PRODUCER_CONFIG = {
    # Idempotence = acks=all + bounded in-flight + sequence numbers: broker-side retries can't duplicate or reorder.
    "enable.idempotence": True,
    "acks": "all",
    "max.in.flight.requests.per.connection": 5,
    "compression.type": "zstd",
    "linger.ms": 20,
    # Same hash as the Java client's default partitioner, so JVM consumers/Kafka Streams co-partition by key.
    "partitioner": "murmur2_random",
}


def make_producer(bootstrap: str, **overrides) -> Producer:
    return Producer({"bootstrap.servers": bootstrap, **PRODUCER_CONFIG, **overrides})


def serialize(event: dict) -> tuple[bytes | None, bytes]:
    key = event.get("vehicle_id")
    return (key.encode() if isinstance(key, str) else None), json.dumps(event, separators=(",", ":")).encode()


def publish(producer: Producer, events: Iterable[dict], topic: str, dlq_topic: str) -> Counter:
    """Send events and wait until every one is acknowledged. Returns counts of {sent, dead_lettered, delivered}."""
    stats: Counter = Counter()

    def on_delivery(err, _msg):
        stats["failed" if err else "delivered"] += 1

    for event in events:
        key, value = serialize(event)
        problems = contract.errors(event)
        if problems:
            target, headers = dlq_topic, [("error", "; ".join(problems).encode())]
            stats["dead_lettered"] += 1
        else:
            target, headers = topic, None
            stats["sent"] += 1
        while True:
            try:
                producer.produce(target, key=key, value=value, headers=headers, on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: serve delivery callbacks, then retry
                producer.poll(0.5)
        producer.poll(0)
    if producer.flush(60):
        raise KafkaException("timed out waiting for delivery of all messages")
    if stats["failed"]:
        raise KafkaException(f"{stats['failed']} messages were not delivered")
    return stats
