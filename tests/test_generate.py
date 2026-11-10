import hashlib
from collections import Counter
from datetime import datetime, timedelta

from streamhouse import contract
from streamhouse.generate import DEFAULT_START, generate, unique_valid


def _ts(e: dict) -> datetime:
    return datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))


def test_deterministic_for_a_seed():
    digest = lambda arr: hashlib.sha256("".join(a.to_json() + a.kind for a in arr).encode()).hexdigest()  # noqa: E731
    assert digest(generate(seed=7, vehicles=5, hours=1)) == digest(generate(seed=7, vehicles=5, hours=1))
    assert digest(generate(seed=7, vehicles=5, hours=1)) != digest(generate(seed=8, vehicles=5, hours=1))


def test_arrivals_cover_every_messy_case_and_respect_the_contract():
    arrivals = generate(vehicles=20, hours=2)
    kinds = Counter(a.kind for a in arrivals)
    assert set(kinds) == {"ok", "late_minor", "late_major", "duplicate", "invalid"}
    assert [a.arrival_ts for a in arrivals] == sorted(a.arrival_ts for a in arrivals)
    for a in arrivals:
        assert bool(contract.errors(a.payload)) == (a.kind == "invalid"), a


def test_duplicates_resend_an_earlier_payload_and_late_events_are_late():
    arrivals = generate(vehicles=20, hours=2)
    seen: set[str] = set()
    for a in arrivals:
        if a.kind == "duplicate":
            assert a.payload["event_id"] in seen
        seen.add(a.payload.get("event_id"))
        lateness = a.arrival_ts - _ts(a.payload) if a.kind != "invalid" else None
        if a.kind == "late_major":
            assert lateness > timedelta(minutes=10)  # behind the watermark
        if a.kind == "late_minor":
            assert timedelta(minutes=1) <= lateness <= timedelta(minutes=5)
    truth = unique_valid(arrivals)
    assert len({e["event_id"] for e in truth}) == len(truth)


def test_schema_evolves_midway():
    truth = unique_valid(generate(vehicles=10, hours=2))
    midpoint = DEFAULT_START + timedelta(hours=1)
    for e in truth:
        assert e["schema_version"] == (2 if _ts(e) >= midpoint else 1)
        assert ("battery_temp_c" in e) == (e["schema_version"] == 2)
