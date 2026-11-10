import pytest
from pyspark.sql import types as T

from conftest import event
from streamhouse import contract


def test_valid_event_passes():
    assert contract.errors(event("a1", "10:00")) == []
    assert contract.errors(event("a1", "10:00", version=2, battery_temp_c=31.5)) == []


@pytest.mark.parametrize(
    ("change", "fragment"),
    [
        ({"speed_kmh": -3}, "speed_kmh"),
        ({"event_ts": "yesterday-ish"}, "event_ts"),
        ({"firmware": "7.1"}, "Additional properties"),
        ({"vehicle_id": "truck 9"}, "vehicle_id"),
        ({"schema_version": 3}, "schema_version"),
    ],
)
def test_violations_are_reported(change, fragment):
    assert any(fragment in err for err in contract.errors(event("a1", "10:00") | change))


def test_missing_required_field():
    e = event("a1", "10:00")
    del e["lat"]
    assert contract.errors(e) == ["<root>: 'lat' is a required property"]


def test_spark_schema_follows_contract_versions():
    v1, v2 = contract.spark_schema(1), contract.spark_schema(2)
    assert "battery_temp_c" not in v1.fieldNames()
    assert v2.fieldNames() == v1.fieldNames() + ["battery_temp_c"]
    assert v2["event_ts"].dataType == T.TimestampType()
    assert v2["speed_kmh"].dataType == T.DoubleType()
