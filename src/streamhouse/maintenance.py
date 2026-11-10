"""Batch side of the lakehouse: gold rebuild, data-quality gates and Iceberg table maintenance.
Each function is one Airflow task (dags/streamhouse_maintenance.py) and one CLI subcommand."""

from datetime import UTC, date, datetime, timedelta

from pyspark.sql import SparkSession

from streamhouse import contract, tables
from streamhouse.config import CATALOG, SPEEDING_KMH


class DataQualityError(RuntimeError):
    pass


def data_files(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT count(*) FROM {table}.files WHERE content = 0").first()[0]


def snapshots(spark: SparkSession, table: str) -> list[tuple[int, datetime, str]]:
    rows = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table}.snapshots ORDER BY committed_at")
    return [tuple(r) for r in rows.collect()]


def rebuild_gold_daily(spark: SparkSession, days: list[date] | None = None) -> int:
    """Recompute gold.fleet_daily from silver. Dynamic partition overwrite replaces only the days touched,
    so a rerun is idempotent and late events that the streaming windows dropped are counted here."""
    where = f"WHERE to_date(event_ts) IN ({', '.join(repr(d.isoformat()) for d in days)})" if days else ""
    spark.sql(f"""
        INSERT OVERWRITE {tables.GOLD_DAILY}
        SELECT to_date(event_ts) AS day, vehicle_id, count(*) AS pings,
               max(odometer_km) - min(odometer_km) AS distance_km,
               avg(speed_kmh) AS avg_speed_kmh, max(speed_kmh) AS max_speed_kmh,
               count_if(speed_kmh > {SPEEDING_KMH}) AS speeding_pings
        FROM {tables.SILVER} {where}
        GROUP BY 1, 2""")
    return spark.table(tables.GOLD_DAILY).count()


DQ_CHECKS = {
    "silver_duplicate_event_ids": f"SELECT count(*) - count(DISTINCT event_id) FROM {tables.SILVER}",
    "silver_null_required": f"SELECT count(*) FROM {tables.SILVER} WHERE "
    + " OR ".join(f"{c} IS NULL" for c in contract.required()),
    "silver_speed_out_of_range": f"SELECT count(*) FROM {tables.SILVER} WHERE speed_kmh < 0 OR speed_kmh > 250",
    "vehicle_state_not_latest": f"""
        SELECT count(*) FROM {tables.STATE} s
        JOIN (SELECT vehicle_id, max(event_ts) AS ts FROM {tables.SILVER} GROUP BY 1) m USING (vehicle_id)
        WHERE s.event_ts <> m.ts""",
    "gold_daily_unreconciled_pings": f"""
        SELECT abs((SELECT coalesce(sum(pings), 0) FROM {tables.GOLD_DAILY})
                 - (SELECT count(*) FROM {tables.SILVER}))""",
    # Streaming gold may under-count (late data behind the watermark) but must never over-count.
    "gold_5min_overcounted_windows": f"""
        SELECT count(*) FROM {tables.GOLD_5MIN} g
        JOIN (SELECT window(event_ts, '5 minutes').start AS window_start, vehicle_id, count(*) AS n
              FROM {tables.SILVER} GROUP BY 1, 2) s USING (window_start, vehicle_id)
        WHERE g.pings > s.n""",
}


def dq_checks(spark: SparkSession) -> dict[str, int]:
    """Run every check; each returns the number of violating rows. Raises if any is non-zero."""
    results = {name: int(spark.sql(sql).first()[0]) for name, sql in DQ_CHECKS.items()}
    failed = {k: v for k, v in results.items() if v}
    if failed:
        raise DataQualityError(f"data-quality checks failed: {failed}")
    return results


def compact(spark: SparkSession, table: str, target_file_mb: int = 128) -> dict:
    """Bin-pack small files written by micro-batches into ~target-size files (rewrite_data_files)."""
    before = data_files(spark, table)
    r = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{table}',
            options => map('min-input-files', '2',
                           'target-file-size-bytes', '{target_file_mb * 1024 * 1024}'))""").first()
    return {
        "table": table,
        "files_before": before,
        "files_after": data_files(spark, table),
        "rewritten": r["rewritten_data_files_count"],
        "added": r["added_data_files_count"],
    }


def expire_snapshots(spark: SparkSession, table: str, keep_days: int = 7, retain_last: int = 5) -> dict:
    """Drop snapshots older than `keep_days` (keeping at least `retain_last`) and delete files only they used.
    This bounds time travel to `keep_days`, which is the point: storage cost vs rollback window."""
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
    r = spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{table}', older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})""").first()
    return {
        "table": table,
        "deleted_data_files": r["deleted_data_files_count"],
        "snapshots_left": len(snapshots(spark, table)),
    }
