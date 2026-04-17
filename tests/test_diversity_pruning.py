"""Test that the diversity / uniqueness dedup logic does NOT produce
dominated trip alternatives — i.e. alternatives that arrive later
AND have more transfers than the primary.

The C++ engine's post-processing pipeline(commit after 528a277) adds:
1. Increased transfer penalty (300 → 600s)
2. Dominance pruning after diversity dedup
3. Supplementary-mode time cap (1.5× best duration)
4. Phantom-leg filter (same board/alight stop)
"""

import os
import time
from datetime import datetime, timezone, timedelta

import pytest
import requests

ENGINE_URL = os.environ.get("TRACK_ENGINE_URL", "http://localhost:8081")


def _service_ctx() -> dict:
    """Return service day context for the C++ engine."""
    now = int(time.time()) + 300
    ny_tz = timezone(timedelta(hours=-4))
    ny_now = datetime.fromtimestamp(now, tz=ny_tz)
    sdate = (ny_now - timedelta(days=1)).date() if ny_now.hour < 3 else ny_now.date()
    midnight = datetime(sdate.year, sdate.month, sdate.day, tzinfo=ny_tz)
    return {
        "depart_at_ts": now,
        "query_ts": now,
        "service_day_yyyymmdd": int(sdate.strftime("%Y%m%d")),
        "service_weekday": sdate.weekday(),
        "service_day_midnight_ts": int(midnight.timestamp()),
    }


@pytest.fixture
def ctx():
    return _service_ctx()


def _go(origin_lat, origin_lon, dest_lat, dest_lon, ctx, **extra):
    payload = {
        "origin": {"label": "O", "lat": origin_lat, "lon": origin_lon},
        "destination": {"label": "D", "lat": dest_lat, "lon": dest_lon},
        "num_itineraries": 5,
        "max_transfers": 3,
        **ctx,
        **extra,
    }
    r = requests.post(f"{ENGINE_URL}/go", json=payload, timeout=30)
    assert r.status_code == 200, f"Engine returned {r.status_code}: {r.text[:200]}"
    return r.json()


class TestDominancePruning:
    """Verify no alternative is Pareto-dominated by the primary."""

    TRIPS = [
        ("Inwood→BrooklynHeights",  40.8681, -73.9209, 40.6940, -73.9940),
        ("ParkSlope→Midtown",       40.6710, -73.9770, 40.7549, -73.9840),
        ("WashHts→Bushwick",        40.8480, -73.9344, 40.6944, -73.9213),
        ("Dumbo→UWS",               40.6994, -73.9874, 40.7870, -73.9754),
    ]

    @pytest.mark.parametrize("name,o_lat,o_lon,d_lat,d_lon", TRIPS)
    def test_no_dominated_alternative(self, ctx, name, o_lat, o_lon, d_lat, d_lon):
        """Alt that arrives same-or-later with more transfers must not appear."""
        data = _go(o_lat, o_lon, d_lat, d_lon, ctx)
        primary = data.get("primary_trip")
        if not primary:
            pytest.skip("No primary trip returned")
        p_arrive = primary["itinerary"]["arrive_at_ts"]
        p_xfers = primary["itinerary"]["transfer_count"]

        for i, alt in enumerate(data.get("alternatives", [])):
            a_arrive = alt["itinerary"]["arrive_at_ts"]
            a_xfers = alt["itinerary"]["transfer_count"]
            assert not (a_arrive >= p_arrive and a_xfers > p_xfers), (
                f"ALT-{i+1} is dominated: arrives {a_arrive} >= primary {p_arrive} "
                f"with {a_xfers} > {p_xfers} transfers"
            )

    @pytest.mark.parametrize("name,o_lat,o_lon,d_lat,d_lon", TRIPS)
    def test_no_phantom_legs(self, ctx, name, o_lat, o_lon, d_lat, d_lon):
        """No transit leg should board and alight at the same stop."""
        data = _go(o_lat, o_lon, d_lat, d_lon, ctx)
        for label, trip in self._all_trips(data):
            for leg in trip["itinerary"]["legs"]:
                if leg["mode"] == "walk":
                    continue
                assert not (
                    leg["board_stop_id"] == leg["alight_stop_id"]
                    and leg["stop_count"] <= 0
                ), f"{label} has phantom leg on {leg['route_name']}"

    @staticmethod
    def _all_trips(data):
        trips = []
        if data.get("primary_trip"):
            trips.append(("PRIMARY", data["primary_trip"]))
        for i, alt in enumerate(data.get("alternatives", [])):
            trips.append((f"ALT-{i+1}", alt))
        return trips
