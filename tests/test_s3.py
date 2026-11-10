"""The same pipeline against an S3 API (moto's S3 server standing in for MinIO/AWS): Iceberg's S3FileIO
writes data + metadata objects, the streaming job and compaction run unchanged. Runs Spark in a
subprocess because S3FileIO's jars must be on the classpath when the JVM starts."""

import json
import os
import socket
import subprocess
import sys

import pytest

from conftest import Replay, event
from streamhouse.config import h2_catalog

moto_server = pytest.importorskip("moto.server")
boto3 = pytest.importorskip("boto3")

SCRIPT = """
import json, sys
from streamhouse import maintenance, streaming, tables
from streamhouse.config import Settings
from streamhouse.spark import build_spark

s = Settings()
spark = build_spark(s, app="streamhouse-s3-test")
tables.ensure_tables(spark)
streaming.run_until_drained(streaming.start(spark, streaming.file_source(spark, sys.argv[1]), s.checkpoints, True))
maintenance.rebuild_gold_daily(spark)
print(json.dumps({"silver": spark.table(tables.SILVER).count(), "dq": maintenance.dq_checks(spark),
                  "compaction": maintenance.compact(spark, tables.SILVER)}))
"""


@pytest.fixture
def s3_endpoint():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


def test_pipeline_on_s3(tmp_path, s3_endpoint):
    creds = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1"}
    s3 = boto3.client(
        "s3", endpoint_url=s3_endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    s3.create_bucket(Bucket="lake")

    replay = Replay(tmp_path / "replay")
    replay.add(event("a1", "10:00"), event("a2", "10:01"), event("a1", "10:00"))
    replay.add(event("a3", "10:30", version=2, battery_temp_c=30.0))

    env = (
        os.environ
        | creds
        | {
            "ICEBERG_WAREHOUSE": "s3://lake/warehouse",
            "S3_ENDPOINT": s3_endpoint,
            "ICEBERG_CATALOG_URI": h2_catalog(tmp_path / "catalog"),
            "CHECKPOINT_DIR": str(tmp_path / "checkpoints"),
        }
    )
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(replay.path)], env=env, capture_output=True, text=True, timeout=600
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["silver"] == 3
    assert set(result["dq"].values()) == {0}
    assert result["compaction"]["files_after"] == 1

    keys = [
        o["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket="lake")
        for o in page.get("Contents", [])
    ]
    assert any(
        k.startswith("warehouse/silver/telemetry/data/event_ts_day=2026-11-02/") and k.endswith(".parquet")
        for k in keys
    )
    assert any(k.startswith("warehouse/silver/telemetry/metadata/") and k.endswith(".metadata.json") for k in keys)
    assert any(k.startswith("warehouse/gold/vehicle_5min/") for k in keys)
