"""End-to-end benchmark + correctness eval against the generator's ground truth.

generate -> (Kafka producer | JSON-lines replay) -> streaming bronze/silver/gold (availableNow, many micro-batches)
-> gold daily rebuild -> DQ checks -> time travel -> compaction -> snapshot expiry. Writes reports/bench-<source>.json.
"""

import json
import os
import shutil
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from streamhouse import contract, maintenance, streaming, tables
from streamhouse.config import ROOT, Settings, h2_catalog
from streamhouse.generate import Arrival, generate, unique_valid
from streamhouse.spark import build_spark


def _window_start(event_ts: str) -> int:
    epoch = int(datetime.fromisoformat(event_ts.replace("Z", "+00:00")).timestamp())
    return epoch - epoch % 300


def _write_replay_files(arrivals: list[Arrival], path: Path, batch_size: int) -> Counter:
    """Apply the producer's contract gate, then write valid payloads in arrival order, one file per micro-batch.
    mtimes are set explicitly because the file source orders files by modification time."""
    path.mkdir(parents=True)
    valid = [a for a in arrivals if not contract.errors(a.payload)]
    for i in range(0, len(valid), batch_size):
        f = path / f"part-{i // batch_size:05d}.jsonl"
        f.write_text("\n".join(a.to_json() for a in valid[i : i + batch_size]) + "\n")
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))
    return Counter(sent=len(valid), dead_lettered=len(arrivals) - len(valid))


def _produce_to_kafka(arrivals: list[Arrival], s: Settings) -> Counter:
    from confluent_kafka.admin import AdminClient, NewTopic

    from streamhouse.producer import make_producer, publish

    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    for f in admin.create_topics(
        [NewTopic(t, num_partitions=6, replication_factor=1) for t in (s.topic, s.dlq_topic)]
    ).values():
        f.result()
    return publish(make_producer(s.kafka_bootstrap), (a.payload for a in arrivals), s.topic, s.dlq_topic)


def run(source: str = "files", seed: int = 42, vehicles: int = 100, hours: float = 4.0, batch_size: int = 500) -> dict:
    work = ROOT / "lake" / f"bench-{source}"
    shutil.rmtree(work, ignore_errors=True)
    run_id = int(time.time())
    s = Settings(
        catalog_uri=h2_catalog(work / "catalog"),
        warehouse=os.environ.get("ICEBERG_WAREHOUSE") or f"file://{work}/warehouse",
        checkpoints=str(work / "checkpoints"),
        topic=f"fleet.telemetry.bench-{run_id}",
        dlq_topic=f"fleet.telemetry.bench-{run_id}.dlq",
    )
    arrivals = generate(seed=seed, vehicles=vehicles, hours=hours)
    truth = unique_valid(arrivals)
    kinds = Counter(a.kind for a in arrivals)
    report: dict = {
        "source": source,
        "warehouse": s.warehouse.split("://")[0],
        "seed": seed,
        "arrivals": len(arrivals),
        "by_kind": dict(kinds),
        "unique_valid_events": len(truth),
    }

    spark = build_spark(
        s, app=f"streamhouse-bench-{source}", **{"spark.sql.streaming.numRecentProgressUpdates": "10000"}
    )
    tables.ensure_tables(spark)

    t0 = time.perf_counter()
    if source == "kafka":
        produced = _produce_to_kafka(arrivals, s)
        raw = streaming.kafka_source(spark, s, max_offsets_per_trigger=batch_size)
    else:
        produced = _write_replay_files(arrivals, work / "replay", batch_size)
        raw = streaming.file_source(spark, str(work / "replay"))
    produce_s = time.perf_counter() - t0
    report["ingest"] = {**produced, "seconds": round(produce_s, 2), "events_per_sec": round(len(arrivals) / produce_s)}

    t0 = time.perf_counter()
    queries = streaming.start(spark, raw, s.checkpoints, available_now=True)
    streaming.run_until_drained(queries)
    stream_s = time.perf_counter() - t0
    progress = {q.name: q.recentProgress for q in queries}
    input_rows = sum(p["numInputRows"] for p in progress["silver"])
    report["streaming"] = {
        "seconds": round(stream_s, 2),
        "input_rows": input_rows,
        "micro_batches": len([p for p in progress["silver"] if p["numInputRows"]]),
        "events_per_sec": round(input_rows / stream_s),
    }

    # --- silver: exactly the unique valid events, nothing more ---
    silver = spark.table(tables.SILVER)
    silver_ids = {r[0] for r in silver.select("event_id").collect()}
    truth_ids = {e["event_id"] for e in truth}
    v2_truth = sum(1 for e in truth if e["schema_version"] == 2)
    report["silver"] = {
        "rows": silver.count(),
        "missing_vs_truth": len(truth_ids - silver_ids),
        "unexpected_vs_truth": len(silver_ids - truth_ids),
        "duplicates_in_input": kinds["duplicate"],
        "duplicates_removed": input_rows - silver.count(),
        "v2_rows_with_battery_temp": silver.where("battery_temp_c IS NOT NULL").count(),
        "v2_rows_expected": v2_truth,
    }
    latest: dict[str, dict] = {}
    for e in truth:
        if e["vehicle_id"] not in latest or e["event_ts"] > latest[e["vehicle_id"]]["event_ts"]:
            latest[e["vehicle_id"]] = e
    state = {r.vehicle_id: r.event_id for r in spark.table(tables.STATE).collect()}
    report["vehicle_state"] = {
        "vehicles": len(state),
        "wrong_latest": sum(state.get(v) != e["event_id"] for v, e in latest.items()),
    }

    # --- streaming gold: what the watermark dropped, and whether only too-late events were dropped ---
    truth_w, major_w = Counter(), Counter()
    late_major_ids = {a.payload["event_id"] for a in arrivals if a.kind == "late_major"}
    for e in truth:
        key = (_window_start(e["event_ts"]), e["vehicle_id"])
        truth_w[key] += 1
        major_w[key] += e["event_id"] in late_major_ids
    gold = {(int(r.window_start.timestamp()), r.vehicle_id): r.pings for r in spark.table(tables.GOLD_5MIN).collect()}
    deficits = {k: truth_w[k] - n for k, n in gold.items()}
    report["gold_stream"] = {
        "windows_emitted": len(gold),
        "windows_still_open": len(truth_w) - len(gold),
        "events_in_emitted_windows": sum(truth_w[k] for k in gold),
        "events_dropped_as_late": sum(deficits.values()),
        "late_major_in_emitted_windows": sum(major_w[k] for k in gold),
        "dropped_not_explained_by_late_major": sum(max(0, d - major_w[k]) for k, d in deficits.items()),
        "overcounted_windows": sum(d < 0 for d in deficits.values()),
    }

    # --- batch gold rebuild recovers the late events ---
    t0 = time.perf_counter()
    maintenance.rebuild_gold_daily(spark)
    daily_truth = Counter((e["event_ts"][:10], e["vehicle_id"]) for e in truth)
    daily = {(r.day.isoformat(), r.vehicle_id): r.pings for r in spark.table(tables.GOLD_DAILY).collect()}
    report["gold_daily"] = {
        "seconds": round(time.perf_counter() - t0, 2),
        "rows": len(daily),
        "pings": sum(daily.values()),
        "mismatched_rows_vs_truth": sum(daily.get(k) != n for k, n in daily_truth.items()),
    }
    report["dq_checks"] = maintenance.dq_checks(spark)

    # --- time travel: the first silver snapshot is still queryable ---
    snaps = maintenance.snapshots(spark, tables.SILVER)
    first = snaps[0][0]
    report["time_travel"] = {
        "silver_snapshots": len(snaps),
        "first_snapshot_rows": spark.sql(f"SELECT count(*) FROM {tables.SILVER} VERSION AS OF {first}").first()[0],
        "current_rows": silver.count(),
    }

    # --- maintenance: compaction, then expiry deletes the files only old snapshots referenced ---
    report["compaction"] = []
    for table in (tables.SILVER, tables.GOLD_5MIN, tables.BRONZE):
        t0 = time.perf_counter()
        r = maintenance.compact(spark, table)
        report["compaction"].append(r | {"seconds": round(time.perf_counter() - t0, 2)})
    report["expiry"] = maintenance.expire_snapshots(spark, tables.SILVER, keep_days=0, retain_last=1)
    spark.stop()

    out = ROOT / "reports" / f"bench-{source}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report
