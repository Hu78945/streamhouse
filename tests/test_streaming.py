"""Structured Streaming behaviour with real local Spark + Iceberg: watermark/late data, dedup,
out-of-order upserts, restart from checkpoint, and crash-replay idempotence."""

import pytest

from conftest import event
from streamhouse import streaming, tables

# One vehicle, 10 min watermark. Gold chains two stateful operators (dedup -> window aggregation); for chained
# operators Spark filters late rows against the watermark of the *previous* micro-batch, so a row is dropped
# once it is older than (max event time two batches back) - 10 min.
A0, A1, A2 = event("a0", "10:00", speed=40), event("a1", "10:06", speed=60), event("a2", "10:20", speed=80)
LATE_OK = event("b1", "10:12", speed=70)  # batch 1, watermark 10:10: on time for gold
LATE_DROP = event("b2", "10:03", speed=90)  # batch 2, late-row watermark 10:10: behind it, gold drops it
A3 = event("a3", "10:40", speed=30)
LATE_STALE = event("b3", "10:25", speed=10)  # arrives after A3: must not overwrite vehicle_state


def _feed_first_half(replay):
    replay.add(A0, A1, A2)
    replay.add(A1, LATE_OK, "{not json")  # A1 again = duplicate resend; garbage: bronze keeps it, silver drops it
    replay.add(LATE_DROP)


def _run(spark, replay, checkpoints):
    streaming.run_until_drained(
        streaming.start(spark, streaming.file_source(spark, str(replay.path)), str(checkpoints), available_now=True)
    )


def _gold(spark):
    rows = spark.table(tables.GOLD_5MIN).selectExpr("date_format(window_start, 'HH:mm') AS w", "pings").collect()
    return {(r.w, r.pings) for r in rows}


def test_watermark_dedup_and_restart_from_checkpoint(lake, replay, tmp_path):
    cp = tmp_path / "checkpoints"
    _feed_first_half(replay)
    _run(lake, replay, cp)
    assert lake.table(tables.SILVER).count() == 5  # A0 A1 A2 LATE_OK LATE_DROP, the resend skipped
    assert _gold(lake) == {("10:00", 1), ("10:05", 1)}  # closed windows only; A1's resend not double-counted

    replay.add(A3)
    replay.add(LATE_STALE)
    _run(lake, replay, cp)  # restart: resumes from the checkpoint, nothing re-read

    assert lake.table(tables.BRONZE).count() == 3 + 3 + 1 + 1 + 1  # every raw line exactly once, garbage included
    silver = {r.event_id[:2] for r in lake.table(tables.SILVER).collect()}
    assert silver == {"a0", "a1", "a2", "b1", "b2", "a3", "b3"}  # silver keeps late data, even behind the watermark
    # 10:00 misses LATE_DROP (dropped behind the watermark); 10:10 includes LATE_OK. LATE_STALE (10:25) is 15 min
    # behind A3 but its batch's late-row watermark is still 10:10, so it counts. 10:40 is still open.
    assert _gold(lake) == {("10:00", 1), ("10:05", 1), ("10:10", 1), ("10:20", 1), ("10:25", 1)}
    state = lake.table(tables.STATE).collect()
    assert [(r.event_id[:2], r.speed_kmh) for r in state] == [("a3", 30.0)]


def test_crash_between_merges_replays_to_the_same_state(lake, replay, tmp_path, monkeypatch):
    cp = tmp_path / "checkpoints"
    _feed_first_half(replay)
    real = streaming._merge_vehicle_state

    def crash(*_):  # silver.telemetry is committed, then the job dies before the state MERGE
        raise RuntimeError("simulated executor loss")

    monkeypatch.setattr(streaming, "_merge_vehicle_state", crash)
    with pytest.raises(Exception, match="simulated executor loss"):
        _run(lake, replay, cp)
    assert lake.table(tables.SILVER).count() == 3  # batch 0 telemetry committed; its checkpoint was not
    assert lake.table(tables.STATE).count() == 0

    monkeypatch.setattr(streaming, "_merge_vehicle_state", real)
    _run(lake, replay, cp)  # Spark replays batch 0, then continues
    rows = lake.table(tables.SILVER)
    assert rows.count() == rows.select("event_id").distinct().count() == 5
    assert [r.event_id[:2] for r in lake.table(tables.STATE).collect()] == ["a2"]


def test_parse_drops_unparseable_and_keeps_v1_v2(spark):
    import json

    raw = spark.createDataFrame(
        [
            (json.dumps(A0),),
            (json.dumps(event("c1", "10:01", version=2, battery_temp_c=33.0)),),
            ("{bad",),
            (json.dumps({"event_id": "x"}),),
        ],
        "value string",
    )
    rows = {r.event_id[:2]: r.battery_temp_c for r in streaming.parse(raw).collect()}
    assert rows == {"a0": None, "c1": 33.0}
