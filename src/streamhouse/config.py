"""Runtime settings, all overridable from the environment (see .env.example)."""

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = "lake"

WATERMARK = "10 minutes"  # how long gold windows wait for late events before finalizing
WINDOW = "5 minutes"
SPEEDING_KMH = 120.0


def h2_catalog(path: Path | str) -> str:
    """JDBC URL for an embedded H2 catalog file. AUTO_SERVER lets several processes (CLI, Airflow tasks) share it;
    lower-case identifiers are what Iceberg's JdbcCatalog expects when it probes its own schema."""
    return f"jdbc:h2:file:{path};AUTO_SERVER=TRUE;DATABASE_TO_LOWER=TRUE"


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "fleet.telemetry"))
    dlq_topic: str = field(default_factory=lambda: _env("KAFKA_DLQ_TOPIC", "fleet.telemetry.dlq"))
    # Iceberg catalog: a JDBC URL (embedded H2 locally) or `glue` (AWS) + warehouse location (local dir or s3://bucket/prefix)
    catalog_uri: str = field(default_factory=lambda: _env("ICEBERG_CATALOG_URI", h2_catalog(ROOT / "lake" / "catalog")))
    warehouse: str = field(default_factory=lambda: _env("ICEBERG_WAREHOUSE", f"file://{ROOT}/lake/warehouse"))
    checkpoints: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", str(ROOT / "lake" / "checkpoints")))
    s3_endpoint: str | None = field(default_factory=lambda: os.environ.get("S3_ENDPOINT") or None)
