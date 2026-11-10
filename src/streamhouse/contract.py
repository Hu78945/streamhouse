"""The telemetry data contract: one JSON Schema drives producer validation and the Spark/Iceberg schema."""

import json
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator
from pyspark.sql import types as T

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "telemetry.schema.json"
LATEST_VERSION = 2

_SPARK_TYPES = {"string": T.StringType(), "integer": T.IntegerType(), "number": T.DoubleType()}


@cache
def schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


@cache
def _validator() -> Draft202012Validator:
    return Draft202012Validator(schema())


def errors(event: object) -> list[str]:
    """Contract violations for one decoded event; empty list means valid."""
    return sorted(
        f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in _validator().iter_errors(event)
    )


def fields(version: int = LATEST_VERSION) -> list[str]:
    """Field names in contract order that exist at `version`."""
    return [name for name, spec in schema()["properties"].items() if spec.get("x-added-in", 1) <= version]


def spark_schema(version: int = LATEST_VERSION) -> T.StructType:
    """Spark schema for `version`. event_ts is parsed to a timestamp; required fields are still nullable
    here because from_json yields nulls for malformed input, which the job filters explicitly."""
    props, out = schema()["properties"], []
    for name in fields(version):
        spec = props[name]
        if spec.get("format") == "date-time":
            dtype = T.TimestampType()
        else:
            json_type = next(
                t for t in (spec["type"] if isinstance(spec["type"], list) else [spec["type"]]) if t != "null"
            )
            dtype = _SPARK_TYPES[json_type]
        out.append(T.StructField(name, dtype, nullable=True))
    return T.StructType(out)


def required() -> list[str]:
    return list(schema()["required"])
