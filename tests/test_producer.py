"""Producer tests against librdkafka's built-in mock cluster (`test.mock.num.brokers`): the real client,
real idempotent produce path and real partitioner, with no broker or Docker."""

import json
import time

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from streamhouse.generate import generate
from streamhouse.producer import make_producer, publish, serialize


def java_murmur2(data: bytes) -> int:
    """org.apache.kafka.common.utils.Utils.murmur2 (the Java client's default key hash), as unsigned 32-bit."""
    m, h = 0x5BD1E995, 0x9747B28C ^ len(data)
    for i in range(0, len(data) - len(data) % 4, 4):
        k = int.from_bytes(data[i : i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        h = ((h * m) & 0xFFFFFFFF) ^ ((k * m) & 0xFFFFFFFF)
    tail, rest = len(data) & ~3, len(data) % 4
    if rest == 3:
        h ^= data[tail + 2] << 16
    if rest >= 2:
        h ^= data[tail + 1] << 8
    if rest >= 1:
        h = ((h ^ data[tail]) * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    return h ^ (h >> 15)


@pytest.mark.parametrize(
    ("text", "expected"),  # vectors from Kafka's UtilsTest
    [
        ("21", -973932308),
        ("foobar", -790332482),
        ("a-little-bit-long-string", -985981536),
        ("a-little-bit-longer-string", -1486304829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        ("abc", 479470107),
    ],
)
def test_java_murmur2_reference(text, expected):
    assert java_murmur2(text.encode()) == expected & 0xFFFFFFFF


def test_serialize_keys_by_vehicle():
    key, value = serialize({"vehicle_id": "veh-0007", "speed_kmh": 1.5})
    assert key == b"veh-0007"
    assert json.loads(value) == {"vehicle_id": "veh-0007", "speed_kmh": 1.5}
    assert serialize({"speed_kmh": 1.5})[0] is None  # keyless (invalid) events still serialize for the DLQ


def _drain(bootstrap: str, topic: str, partitions: int, expected: int) -> list:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "test", "enable.auto.commit": False})
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(partitions)])
    out, deadline = [], time.time() + 30
    while len(out) < expected and time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            out.append(msg)
    consumer.close()
    return out


def test_publish_routes_valid_and_dead_letters_against_mock_cluster():
    producer = make_producer("unused:9092", **{"test.mock.num.brokers": 3})
    assert producer.list_topics(timeout=10).brokers  # mock cluster is up
    arrivals = generate(seed=3, vehicles=12, hours=0.5)
    invalid = sum(a.kind == "invalid" for a in arrivals)
    assert invalid > 0

    stats = publish(producer, (a.payload for a in arrivals), "telemetry", "telemetry.dlq")
    assert stats == {"sent": len(arrivals) - invalid, "dead_lettered": invalid, "delivered": len(arrivals)}

    md = producer.list_topics(timeout=10)
    broker = next(iter(md.brokers.values()))
    bootstrap = f"{broker.host}:{broker.port}"
    n_parts = len(md.topics["telemetry"].partitions)
    assert n_parts > 1

    main = _drain(bootstrap, "telemetry", n_parts, stats["sent"])
    assert len(main) == stats["sent"]
    partitions_by_key: dict[bytes, set[int]] = {}
    for msg in main:
        partitions_by_key.setdefault(msg.key(), set()).add(msg.partition())
    assert len(partitions_by_key) == 12
    for key, parts in partitions_by_key.items():
        # one partition per vehicle (per-vehicle ordering), chosen exactly like the Java client would
        assert parts == {(java_murmur2(key) & 0x7FFFFFFF) % n_parts}
    assert len(set().union(*partitions_by_key.values())) > 1

    dlq = _drain(bootstrap, "telemetry.dlq", len(md.topics["telemetry.dlq"].partitions), invalid)
    assert len(dlq) == invalid
    assert all(dict(m.headers())["error"] for m in dlq)
