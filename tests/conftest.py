import json
import logging
import os
from pathlib import Path

import pytest

from streamhouse import tables
from streamhouse.config import Settings, h2_catalog

logging.getLogger("py4j").setLevel(logging.WARNING)  # its INFO shutdown message hits a closed stream at exit


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One local SparkSession + Iceberg JDBC (SQLite) catalog for the whole run; tables are reset per test."""
    from streamhouse.spark import build_spark

    root = tmp_path_factory.mktemp("lake")
    s = Settings(catalog_uri=h2_catalog(root / "catalog"), warehouse=f"file://{root}/warehouse")
    session = build_spark(
        s, app="streamhouse-tests", **{"spark.sql.shuffle.partitions": "2", "spark.driver.memory": "1g"}
    )
    yield session
    session.stop()


@pytest.fixture
def lake(spark):
    for t in tables.ALL:
        spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    tables.ensure_tables(spark)
    return spark


class Replay:
    """Writes JSON-lines files that `streaming.file_source` turns into one micro-batch each, in order."""

    def __init__(self, path: Path):
        self.path, self.n = path, 0
        path.mkdir(parents=True, exist_ok=True)

    def add(self, *events: dict | str) -> None:
        """One file = one micro-batch. A str is written verbatim (for malformed lines)."""
        f = self.path / f"part-{self.n:05d}.jsonl"
        f.write_text("".join((e if isinstance(e, str) else json.dumps(e)) + "\n" for e in events))
        os.utime(f, (1_700_000_000 + self.n, 1_700_000_000 + self.n))
        self.n += 1


@pytest.fixture
def replay(tmp_path):
    return Replay(tmp_path / "replay")


def event(event_id: str, ts: str, vehicle: str = "veh-0001", speed: float = 50.0, version: int = 1, **extra) -> dict:
    e = {
        "event_id": event_id.ljust(32, "0"),
        "schema_version": version,
        "vehicle_id": vehicle,
        "event_ts": f"2026-11-02T{ts}:00.000Z",
        "lat": 52.37,
        "lon": 4.89,
        "speed_kmh": speed,
        "odometer_km": 1000.0,
    }
    return e | extra
