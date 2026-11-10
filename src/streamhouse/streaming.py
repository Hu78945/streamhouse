"""Structured Streaming jobs: Kafka -> bronze (raw), silver (deduped MERGE), gold (event-time windows).

Three independent queries read the same topic, each with its own checkpoint, so a slow or failing
sink never blocks the others and each can be replayed on its own.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streamhouse import contract, tables
from streamhouse.config import WATERMARK, WINDOW, Settings

RAW_COLUMNS = ["kafka_key", "value", "topic", "kafka_partition", "kafka_offset", "kafka_ts"]


def kafka_source(spark: SparkSession, s: Settings, max_offsets_per_trigger: int | None = None) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return reader.load().select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value"),
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )


def file_source(spark: SparkSession, path: str, files_per_trigger: int = 1) -> DataFrame:
    """Replay a directory of JSON-lines files as a stream with the same columns as `kafka_source`.
    Used by the tests and for offline backfills; each file becomes one micro-batch."""
    return (
        spark.readStream.option("maxFilesPerTrigger", files_per_trigger)
        .text(path)
        .select(
            F.get_json_object("value", "$.vehicle_id").alias("kafka_key"),
            "value",
            F.lit("file").alias("topic"),
            F.lit(None).cast("int").alias("kafka_partition"),
            F.lit(None).cast("bigint").alias("kafka_offset"),
            F.current_timestamp().alias("kafka_ts"),
        )
    )


def parse(raw: DataFrame, version: int = contract.LATEST_VERSION) -> DataFrame:
    """Decode the JSON value against the contract. The producer already dead-letters contract violations;
    this filter is the backstop for anything that still cannot be parsed (nulls from from_json)."""
    events = raw.select(F.from_json("value", contract.spark_schema(version)).alias("e")).select(
        "e.*", F.current_timestamp().alias("ingested_at")
    )
    ok = F.lit(True)
    for name in contract.required():
        ok = ok & F.col(name).isNotNull()
    return events.where(ok)


def gold_windows(events: DataFrame) -> DataFrame:
    """5-minute per-vehicle aggregates in event time. A window is emitted once the watermark passes its end;
    events arriving behind the watermark are dropped here (the nightly gold rebuild from silver recovers them)."""
    return (
        events.withWatermark("event_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_ts", WINDOW).alias("w"), "vehicle_id")
        .agg(
            F.count("*").alias("pings"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "vehicle_id",
            "pings",
            "avg_speed_kmh",
            "max_speed_kmh",
        )
    )


def _merge_events(spark: SparkSession, cols: list[str]) -> None:
    # Insert-only MERGE: an event_id already in silver is a resend and is skipped. Matching on event_ts too
    # (resends are byte-identical) lets Iceberg prune the target scan to the day partitions in the batch.
    spark.sql(f"""
        MERGE INTO {tables.SILVER} t
        USING silver_batch s
        ON t.event_id = s.event_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join(f"s.{c}" for c in cols)})""")


def _merge_vehicle_state(spark: SparkSession, cols: list[str]) -> None:
    # Upsert the latest ping per vehicle. The event_ts guard makes it order-independent:
    # a late ping can never overwrite a newer state, and replaying a batch is a no-op.
    spark.sql(f"""
        MERGE INTO {tables.STATE} t
        USING (
            SELECT {", ".join(cols)} FROM (
                SELECT *, row_number() OVER (PARTITION BY vehicle_id ORDER BY event_ts DESC, event_id DESC) AS rn
                FROM silver_batch)
            WHERE rn = 1) s
        ON t.vehicle_id = s.vehicle_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join(f"s.{c}" for c in cols)})""")


def write_silver(batch: DataFrame, batch_id: int) -> None:
    """foreachBatch sink. Spark re-runs a micro-batch after a crash (at-least-once); both MERGEs are
    idempotent, so the end state is exactly-once."""
    spark = batch.sparkSession
    # Materialize once: both MERGEs read the same rows, and the plan is cut from the streaming source lineage
    # (without this, Iceberg's runtime filter on the state MERGE fails to resolve vehicle_id).
    batch.dropDuplicates(["event_id"]).localCheckpoint().createOrReplaceTempView("silver_batch")
    cols = [c for c in batch.columns if c in set(spark.table(tables.SILVER).columns)]
    _merge_events(spark, cols)
    _merge_vehicle_state(spark, cols)


def start(spark: SparkSession, raw: DataFrame, checkpoints: str, available_now: bool = False) -> list[StreamingQuery]:
    """Start the bronze, silver and gold queries. `available_now` drains what is there and stops."""
    trigger = {"availableNow": True} if available_now else {"processingTime": "10 seconds"}

    def sink(df: DataFrame, name: str):
        return df.writeStream.queryName(name).trigger(**trigger).option("checkpointLocation", f"{checkpoints}/{name}")

    events = parse(raw)
    return [
        sink(raw.withColumn("ingested_at", F.current_timestamp()), "bronze")
        .format("iceberg")
        .option("fanout-enabled", "true")
        .outputMode("append")
        .toTable(tables.BRONZE),
        sink(events, "silver").foreachBatch(write_silver).start(),
        sink(gold_windows(events), "gold")
        .format("iceberg")
        .option("fanout-enabled", "true")
        .outputMode("append")
        .toTable(tables.GOLD_5MIN),
    ]


def run_until_drained(queries: list[StreamingQuery]) -> None:
    """Wait for every query (so none is left running), then surface the first failure."""
    errors = []
    for q in queries:
        try:
            q.awaitTermination()
        except Exception as e:  # StreamingQueryException from a failed micro-batch
            errors.append(e)
    if errors:
        raise errors[0]
