"""Iceberg table behaviour: MERGE upserts, schema evolution, hidden partitioning, time travel,
compaction + snapshot expiry, the gold rebuild and the DQ gate."""

import json
from datetime import date

import pytest

from conftest import event
from streamhouse import maintenance, streaming, tables


def write(spark, *events):
    """Run the silver foreachBatch sink on a plain DataFrame, exactly as a micro-batch would."""
    raw = spark.createDataFrame([(json.dumps(e),) for e in events], "value string")
    streaming.write_silver(streaming.parse(raw), batch_id=0)


def test_vehicle_state_upsert_is_order_independent_and_replay_safe(lake):
    write(
        lake, event("a1", "10:00", speed=10), event("a2", "10:05", speed=20), event("b1", "10:01", vehicle="veh-0002")
    )
    write(lake, event("a0", "09:55", speed=99))  # older ping arriving later: must not win
    write(lake, event("a0", "09:55", speed=99))  # replayed batch: no-op
    state = {r.vehicle_id: (r.event_id[:2], r.speed_kmh) for r in lake.table(tables.STATE).collect()}
    assert state == {"veh-0001": ("a2", 20.0), "veh-0002": ("b1", 50.0)}
    assert lake.table(tables.SILVER).count() == 4


def test_additive_schema_evolution_and_time_travel(lake):
    for t in (tables.SILVER, tables.STATE):
        lake.sql(f"DROP TABLE {t} PURGE")
    assert tables.ensure_tables(lake, version=1) == []
    assert "battery_temp_c" not in lake.table(tables.SILVER).columns
    write(lake, event("a1", "10:00"), event("a2", "10:01"))
    v1_snapshot = maintenance.snapshots(lake, tables.SILVER)[-1][0]

    added = tables.ensure_tables(lake)  # contract v2 deployed
    assert added == [f"{tables.SILVER}.battery_temp_c", f"{tables.STATE}.battery_temp_c"]
    assert tables.ensure_tables(lake) == []  # idempotent
    write(lake, event("a3", "10:02", version=2, battery_temp_c=36.6))

    rows = {r.event_id[:2]: r.battery_temp_c for r in lake.table(tables.SILVER).collect()}
    assert rows == {"a1": None, "a2": None, "a3": 36.6}
    old = lake.sql(f"SELECT * FROM {tables.SILVER} VERSION AS OF {v1_snapshot}")
    assert old.count() == 2
    assert "battery_temp_c" not in old.columns  # time travel reads with the snapshot's own schema


def test_hidden_day_partitioning(lake):
    write(
        lake,
        event("a1", "23:59"),
        event("a2", "23:58"),
        {**event("a3", "00:00"), "event_ts": "2026-11-03T00:01:00.000Z"},
    )
    parts = lake.sql(f"SELECT partition.event_ts_day AS day, record_count FROM {tables.SILVER}.partitions").collect()
    assert {(str(r.day), r.record_count) for r in parts} == {("2026-11-02", 2), ("2026-11-03", 1)}
    # Queries filter on event_ts itself; Iceberg maps that to the day partition (no derived column to keep in sync).
    assert lake.sql(f"SELECT count(*) FROM {tables.SILVER} WHERE event_ts >= '2026-11-03'").first()[0] == 1


def test_compaction_then_expiry_reclaims_small_files(lake):
    for i in range(6):
        write(lake, event(f"a{i}", f"10:0{i}"))
    assert maintenance.data_files(lake, tables.SILVER) == 6
    first = maintenance.snapshots(lake, tables.SILVER)[0][0]

    result = maintenance.compact(lake, tables.SILVER)
    assert (result["files_before"], result["files_after"], result["rewritten"]) == (6, 1, 6)
    assert lake.table(tables.SILVER).count() == 6
    assert lake.sql(f"SELECT count(*) FROM {tables.SILVER} VERSION AS OF {first}").first()[0] == 1

    expired = maintenance.expire_snapshots(lake, tables.SILVER, keep_days=0, retain_last=1)
    assert expired["snapshots_left"] == 1
    assert expired["deleted_data_files"] == 6  # the small files only old snapshots still referenced
    assert lake.table(tables.SILVER).count() == 6


def test_gold_rebuild_is_idempotent_and_partial(lake):
    write(
        lake,
        event("a1", "10:00", speed=130),
        event("a2", "10:30", speed=60),
        {**event("a3", "00:00"), "event_ts": "2026-11-03T00:05:00.000Z", "odometer_km": 1012.5},
    )
    assert maintenance.rebuild_gold_daily(lake) == 2
    assert maintenance.rebuild_gold_daily(lake) == 2  # rerun replaces, never appends
    day1 = lake.table(tables.GOLD_DAILY).where("day = '2026-11-02'").first()
    assert (day1.pings, day1.max_speed_kmh, day1.speeding_pings, day1.avg_speed_kmh) == (2, 130.0, 1, 95.0)

    write(lake, event("a4", "11:00"))
    maintenance.rebuild_gold_daily(lake, [date(2026, 11, 2)])  # only that day's partition is replaced
    counts = {str(r.day): r.pings for r in lake.table(tables.GOLD_DAILY).collect()}
    assert counts == {"2026-11-02": 3, "2026-11-03": 1}


def test_dq_checks_pass_on_clean_data_and_catch_corruption(lake):
    write(lake, event("a1", "10:00"), event("a2", "10:01"))
    maintenance.rebuild_gold_daily(lake)
    assert set(maintenance.dq_checks(lake).values()) == {0}

    lake.sql(f"INSERT INTO {tables.SILVER} SELECT * FROM {tables.SILVER} LIMIT 1")  # a duplicate sneaks in
    with pytest.raises(maintenance.DataQualityError) as err:
        maintenance.dq_checks(lake)
    assert "silver_duplicate_event_ids" in str(err.value)
    assert "gold_daily_unreconciled_pings" in str(err.value)
