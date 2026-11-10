"""SparkSession wired to an Iceberg catalog named `lake`.

Locally the catalog is Iceberg's JdbcCatalog on an embedded H2 file. SQLite was tried first and failed under
the streaming job: each streaming query runs in a cloned SparkSession with its own catalog connection pool,
and concurrent commits from the three queries hit SQLITE_BUSY_SNAPSHOT. H2 (MVCC, AUTO_SERVER for
multi-process access) handles that. In AWS the same code points at the Glue catalog (see README).
"""

import os
import sys
from glob import glob

from pyspark.sql import SparkSession

from streamhouse.config import CATALOG, ROOT, Settings

SPARK_VERSION, ICEBERG_VERSION = "3.5.9", "1.10.1"
PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    "com.h2database:h2:2.5.252",
]
S3_PACKAGE = f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"


def _use_local_jdk() -> None:
    """Prefer a JDK unpacked into .tools/ when JAVA_HOME is not set (Iceberg 1.10 needs Java 11+)."""
    if os.environ.get("JAVA_HOME"):
        return
    for home in sorted(
        glob(str(ROOT / ".tools" / "jdk-17*" / "Contents" / "Home")) + glob(str(ROOT / ".tools" / "jdk-17*"))
    ):
        if os.path.exists(os.path.join(home, "bin", "java")):
            os.environ["JAVA_HOME"] = home
            return


def build_spark(settings: Settings | None = None, app: str = "streamhouse", **extra: str) -> SparkSession:
    s = settings or Settings()
    _use_local_jdk()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers must match the driver's Python
    on_s3 = s.warehouse.startswith("s3")
    cat = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.jars.packages": ",".join(PACKAGES + ([S3_PACKAGE] if on_s3 else [])),
        "spark.jars.ivy": str(ROOT / ".tools" / "ivy2"),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": CATALOG,
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.warehouse": s.warehouse,
        # Streaming queries run in cloned sessions with their own catalog instance; a cached table here
        # would keep serving the snapshot it first loaded and hide their commits.
        f"{cat}.cache-enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "2g",
    }
    if on_s3:
        conf |= {
            f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{cat}.client.region": os.environ.get("AWS_REGION", "us-east-1"),
        }
        if s.s3_endpoint:  # MinIO / moto: path-style addressing against a custom endpoint
            conf |= {f"{cat}.s3.endpoint": s.s3_endpoint, f"{cat}.s3.path-style-access": "true"}
    if s.catalog_uri == "glue":  # AWS: Glue Data Catalog (databases from terraform/), warehouse on S3
        conf |= {f"{cat}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog"}
    else:
        conf |= {
            f"{cat}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            f"{cat}.uri": s.catalog_uri,
            f"{cat}.jdbc.schema-version": "V1",
        }
        if s.catalog_uri.startswith("jdbc:h2:"):
            conf |= {f"{cat}.jdbc.user": "sa", f"{cat}.jdbc.password": ""}
    builder = SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName(app)
    for key, value in (conf | extra).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
