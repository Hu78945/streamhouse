"""Iceberg DDL for bronze/silver/gold, plus contract-driven schema evolution."""

from pyspark.sql import SparkSession

from streamhouse import contract

BRONZE = "bronze.telemetry_raw"
SILVER = "silver.telemetry"
STATE = "silver.vehicle_state"
GOLD_5MIN = "gold.vehicle_5min"
GOLD_DAILY = "gold.fleet_daily"
ALL = [BRONZE, SILVER, STATE, GOLD_5MIN, GOLD_DAILY]

PROPS = "TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')"


def _contract_columns(version: int) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in contract.spark_schema(version)]


def silver_columns(spark: SparkSession, table: str = SILVER) -> list[str]:
    return spark.table(table).columns


def ensure_tables(spark: SparkSession, version: int = contract.LATEST_VERSION) -> list[str]:
    """Create missing tables and add contract fields that the silver tables don't have yet.

    Evolution is additive only (new nullable columns), which Iceberg applies as a metadata-only change:
    no data files are rewritten and older snapshots keep reading with the schema they were written with.
    Returns the `table.column` names that were added.
    """
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            kafka_key STRING, value STRING, topic STRING, kafka_partition INT, kafka_offset BIGINT,
            kafka_ts TIMESTAMP, ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(kafka_ts)) {PROPS}""")
    cols = _contract_columns(version)
    ddl = ", ".join(f"{name} {dtype}" for name, dtype in cols) + ", ingested_at TIMESTAMP"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER} ({ddl}) USING iceberg PARTITIONED BY (days(event_ts)) {PROPS}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE} ({ddl}) USING iceberg {PROPS}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_5MIN} (
            window_start TIMESTAMP, window_end TIMESTAMP, vehicle_id STRING,
            pings BIGINT, avg_speed_kmh DOUBLE, max_speed_kmh DOUBLE)
        USING iceberg PARTITIONED BY (days(window_start)) {PROPS}""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DAILY} (
            day DATE, vehicle_id STRING, pings BIGINT, distance_km DOUBLE,
            avg_speed_kmh DOUBLE, max_speed_kmh DOUBLE, speeding_pings BIGINT)
        USING iceberg PARTITIONED BY (day) {PROPS}""")

    added = []
    for table in (SILVER, STATE):
        existing = set(silver_columns(spark, table))
        for name, dtype in cols:
            if name not in existing:
                spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {dtype}")
                added.append(f"{table}.{name}")
    return added
