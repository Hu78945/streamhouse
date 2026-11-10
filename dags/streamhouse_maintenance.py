"""Nightly lakehouse maintenance: compact -> rebuild gold -> DQ gate -> expire snapshots.

Order matters:
- Compaction first, so the gold rebuild and DQ scans read a few large files instead of thousands of small ones.
- The gold rebuild covers the previous two days: events can arrive up to ~40 min late, so yesterday's
  partition is only final once today's run has seen it again.
- Snapshot expiry runs last and only if DQ passed. While a bad rebuild is still unexpired, it can be
  rolled back with `CALL lake.system.rollback_to_snapshot(...)`.

Tasks call the same `streamhouse.maintenance` functions as the CLI. Here each task starts a local
SparkSession; in AWS the same callables run as EMR Serverless jobs (see terraform/ and README).
"""

from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from streamhouse import tables

COMPACT = [tables.BRONZE, tables.SILVER, tables.GOLD_5MIN]


def _spark():
    from streamhouse.spark import build_spark

    return build_spark(app="streamhouse-maintenance")


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["streamhouse", "iceberg"],
)
def streamhouse_maintenance():
    @task
    def compact(table: str) -> dict:
        from streamhouse.maintenance import compact

        return compact(_spark(), table)

    @task
    def rebuild_gold(ds: str | None = None) -> int:
        from streamhouse.maintenance import rebuild_gold_daily

        day = date.fromisoformat(ds)
        return rebuild_gold_daily(_spark(), [day - timedelta(days=1), day])

    @task(retries=0)  # a failed check is a data problem, not a transient one
    def dq_checks() -> dict:
        from streamhouse.maintenance import dq_checks

        return dq_checks(_spark())

    @task
    def expire(table: str) -> dict:
        from streamhouse.maintenance import expire_snapshots

        return expire_snapshots(_spark(), table)

    compacted = compact.expand(table=COMPACT)
    gate = dq_checks()
    compacted >> rebuild_gold() >> gate >> expire.expand(table=tables.ALL)


streamhouse_maintenance()
