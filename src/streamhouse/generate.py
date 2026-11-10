"""Deterministic synthetic fleet telemetry, emitted in *arrival* order with ground truth.

Each vehicle pings every `interval_s` seconds. Real feeds are messy, so the generator injects:
- out-of-order arrivals: `late_minor` (1-5 min late, inside the 10 min watermark) and
  `late_major` (20-40 min late, behind it),
- `duplicate` resends of an already-sent payload (same event_id, at-least-once device uplink),
- `invalid` payloads that break the contract (they replace the real ping, which is lost),
- schema evolution: pings from the second half of the time range are schema_version 2 and carry
  `battery_temp_c`.
"""

import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

DEFAULT_START = datetime(2026, 11, 2, 22, 0, tzinfo=UTC)  # crosses midnight -> two day partitions

P_LATE_MINOR, P_LATE_MAJOR, P_DUPLICATE, P_INVALID = 0.04, 0.01, 0.03, 0.005


@dataclass(frozen=True)
class Arrival:
    payload: dict
    kind: str  # ok | late_minor | late_major | duplicate | invalid
    arrival_ts: datetime

    def to_json(self) -> str:
        return json.dumps(self.payload, separators=(",", ":"))


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _corrupt(payload: dict, n: int) -> dict:
    bad = dict(payload)
    match n % 4:
        case 0:
            bad["speed_kmh"] = -abs(bad["speed_kmh"]) - 1.0
        case 1:
            bad["event_ts"] = "yesterday-ish"
        case 2:
            del bad["vehicle_id"]
        case 3:
            bad["firmware"] = "7.1.3"  # undeclared field: additionalProperties is false
    return bad


def generate(
    seed: int = 42,
    vehicles: int = 30,
    hours: float = 3.0,
    interval_s: int = 30,
    start: datetime = DEFAULT_START,
) -> list[Arrival]:
    rng = random.Random(seed)
    end = start + timedelta(hours=hours)
    midpoint = start + (end - start) / 2
    out: list[Arrival] = []
    n_invalid = 0
    for v in range(vehicles):
        vehicle_id = f"veh-{v:04d}"
        lat, lon = 52.3 + rng.uniform(-0.2, 0.2), 4.9 + rng.uniform(-0.3, 0.3)
        speed, odometer = rng.uniform(0, 60), rng.uniform(5_000, 150_000)
        ts = start + timedelta(seconds=rng.uniform(0, interval_s))
        while ts < end:
            speed = min(140.0, max(0.0, speed + rng.gauss(0, 8)))
            km = speed * interval_s / 3600
            odometer += km
            lat += rng.uniform(-1, 1) * km / 111
            lon += rng.uniform(-1, 1) * km / 68
            payload = {
                "event_id": f"{rng.getrandbits(128):032x}",
                "schema_version": 2 if ts >= midpoint else 1,
                "vehicle_id": vehicle_id,
                "event_ts": iso(ts),
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kmh": round(speed, 1),
                "odometer_km": round(odometer, 3),
            }
            if payload["schema_version"] == 2:
                payload["battery_temp_c"] = round(rng.uniform(15, 45), 1)

            roll = rng.random()
            if roll < P_INVALID:
                kind, delay = "invalid", rng.uniform(0.2, 3)
                payload = _corrupt(payload, n_invalid)
                n_invalid += 1
            elif roll < P_INVALID + P_LATE_MAJOR:
                kind, delay = "late_major", rng.uniform(1200, 2400)
            elif roll < P_INVALID + P_LATE_MAJOR + P_LATE_MINOR:
                kind, delay = "late_minor", rng.uniform(60, 300)
            else:
                kind, delay = "ok", rng.uniform(0.2, 3)
            arrival = ts + timedelta(seconds=delay)
            out.append(Arrival(payload, kind, arrival))
            if kind != "invalid" and rng.random() < P_DUPLICATE:
                out.append(Arrival(payload, "duplicate", arrival + timedelta(seconds=rng.uniform(1, 120))))
            ts += timedelta(seconds=interval_s)
    out.sort(key=lambda a: (a.arrival_ts, a.payload.get("event_id", "")))
    return out


def unique_valid(arrivals: list[Arrival]) -> list[dict]:
    """Ground truth for silver: every valid event exactly once."""
    return [a.payload for a in arrivals if a.kind not in ("invalid", "duplicate")]
