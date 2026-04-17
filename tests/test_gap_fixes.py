"""
End-to-end integration tests for the 6 RAPTOR/CSA gap fixes.

Each test builds a tailored schedule database, starts the C++ engine,
and asserts the correct behaviour through the /plan HTTP endpoint.

Gap 1 — search_window_minutes is honoured by RAPTOR
Gap 2 — arrive_by_ts filters out late arrivals
Gap 3 — Mode filtering: requesting only "bus" excludes subway results
Gap 4 — num_itineraries cap is respected
Gap 5 — priority scoring: fewer_transfers sorts fewer-transfer trips first
Gap 6 — CSA respects mode filter (bus-only CSA doesn't prune bus paths)
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

NY_TZ = ZoneInfo("America/New_York")

# ──────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE routes (
    route_id TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name TEXT,
    route_color TEXT,
    route_type INTEGER
);
CREATE TABLE trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT,
    service_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER
);
CREATE TABLE stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER
);
CREATE TABLE stops (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_lat REAL,
    stop_lon REAL
);
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date TEXT,
    end_date TEXT
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type INTEGER
);
CREATE INDEX idx_stop_times_stop_dept
    ON stop_times(stop_id, departure_time);
CREATE INDEX idx_stop_times_trip_seq
    ON stop_times(trip_id, stop_sequence);
CREATE INDEX idx_stop_times_trip_stop_seq
    ON stop_times(trip_id, stop_id, stop_sequence);
CREATE INDEX idx_trips_service
    ON trips(service_id);
CREATE INDEX idx_stops_name
    ON stops(stop_name);
"""


def _init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT INTO calendar "
        "VALUES ('WKD',1,1,1,1,1,1,1,'20240101','20300101')"
    )
    return conn


def _timestamp(hour: int, minute: int) -> int:
    now = datetime.now(NY_TZ)
    dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return int(dt.timestamp())


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _base_payload(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    hour: int,
    minute: int,
    **overrides,
) -> dict:
    query_ts = _timestamp(hour, minute)
    service_day = datetime.fromtimestamp(query_ts, NY_TZ).date()
    midnight = datetime.combine(service_day, datetime.min.time(), tzinfo=NY_TZ)
    base = {
        "origin": {"label": "Origin", "lat": origin_lat, "lon": origin_lon},
        "destination": {"label": "Dest", "lat": dest_lat, "lon": dest_lon},
        "depart_at_ts": query_ts,
        "query_ts": query_ts,
        "service_day_midnight_ts": int(midnight.timestamp()),
        "service_day_yyyymmdd": int(service_day.strftime("%Y%m%d")),
        "service_weekday": service_day.weekday(),
        "max_transfers": 2,
        "max_origin_walk_m": 50,
        "max_destination_walk_m": 50,
        "max_transfer_walk_m": 50,
        "search_window_minutes": 180,
        "num_itineraries": 3,
        "modes": ["subway", "bus"],
    }
    base.update(overrides)
    return base


def _build_binary() -> str:
    root = Path(__file__).resolve().parents[1]
    build = subprocess.run(
        [str(root / "scripts" / "build.sh")],
        cwd=root.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    return build.stdout.strip().splitlines()[-1]


def _start_engine(binary: str, schedule_db: Path) -> tuple:
    """Returns (process, port)."""
    port = _free_port()
    process = subprocess.Popen(
        [binary],
        cwd=Path(__file__).resolve().parents[1].parent,
        env={
            **os.environ,
            "PORT": str(port),
            "TRACK_ENGINE_SCHEDULE_DB": str(schedule_db),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 20
    health_url = f"http://127.0.0.1:{port}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as resp:
                data = json.loads(resp.read().decode())
                if data.get("ready"):
                    return process, port
        except Exception:
            time.sleep(0.2)
    output = process.stdout.read() if process.stdout else ""
    process.kill()
    raise AssertionError(f"engine never became healthy\n{output}")


def _plan(port: int, payload: dict) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/plan",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def _stop_engine(process):
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


# Build binary once for the entire module.
_BINARY: str | None = None


@pytest.fixture(scope="module")
def engine_binary():
    global _BINARY
    if _BINARY is None:
        _BINARY = _build_binary()
    return _BINARY


# ──────────────────────────────────────────────────────────────────
#  Gap 1 — search_window_minutes bounds RAPTOR
# ──────────────────────────────────────────────────────────────────
#
#  Two bus trips from A→B:
#    T_EARLY:  depart 08:00 → arrive 08:10  (within 30-min window)
#    T_LATE:   depart 11:00 → arrive 11:10  (outside 30-min window)
#
#  Query: depart_at 07:58, search_window_minutes=30
#  Expected: only T_EARLY appears, T_LATE is excluded.
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap1_search_window_limits_raptor(tmp_path, engine_binary):
    db = tmp_path / "gap1.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("BUS1", "B1", "Bus One", "0078C6", 3),
    ])
    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", [
        ("T_EARLY", "BUS1", "WKD", "Downtown", 0),
        ("T_LATE",  "BUS1", "WKD", "Downtown", 0),
    ])
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SA", "Stop A", 40.7000, -74.0000),
        ("SB", "Stop B", 40.7010, -74.0000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", [
        ("T_EARLY", "08:00:00", "08:00:00", "SA", 1),
        ("T_EARLY", "08:10:00", "08:10:00", "SB", 2),
        ("T_LATE",  "11:00:00", "11:00:00", "SA", 1),
        ("T_LATE",  "11:10:00", "11:10:00", "SB", 2),
    ])
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        payload = _base_payload(
            40.7000, -74.0000, 40.7010, -74.0000, 7, 58,
            search_window_minutes=30,
            modes=["bus"],
            max_transfers=0,
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) >= 1, "should find at least the early trip"
        # Every transit leg's departure should be before query_ts + 30 min
        query_ts = payload["query_ts"]
        cutoff = query_ts + 30 * 60
        for itin in itins:
            for leg in itin["legs"]:
                if leg["mode"] != "walk":
                    assert leg["departure_ts"] <= cutoff, (
                        f"trip departing at {leg['departure_ts']} exceeds "
                        f"search window cutoff {cutoff}"
                    )
    finally:
        _stop_engine(proc)


# ──────────────────────────────────────────────────────────────────
#  Gap 2 — arrive_by_ts filters RAPTOR results
# ──────────────────────────────────────────────────────────────────
#
#  Two bus trips from A→B:
#    T_OK:   depart 08:00 → arrive 08:10
#    T_LATE: depart 08:20 → arrive 08:30
#
#  Query: arrive_by = 08:15 timestamp
#  Expected: only T_OK itinerary appears (arrives 08:10 + walk < 08:15).
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap2_arrive_by_filters_raptor(tmp_path, engine_binary):
    db = tmp_path / "gap2.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("BUS2", "B2", "Bus Two", "0078C6", 3),
    ])
    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", [
        ("T_OK",   "BUS2", "WKD", "Downtown", 0),
        ("T_LATE", "BUS2", "WKD", "Downtown", 0),
    ])
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SA", "Stop A", 40.7000, -74.0000),
        ("SB", "Stop B", 40.7010, -74.0000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", [
        ("T_OK",   "08:00:00", "08:00:00", "SA", 1),
        ("T_OK",   "08:10:00", "08:10:00", "SB", 2),
        ("T_LATE", "08:20:00", "08:20:00", "SA", 1),
        ("T_LATE", "08:30:00", "08:30:00", "SB", 2),
    ])
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        arrive_by = _timestamp(8, 15)
        payload = _base_payload(
            40.7000, -74.0000, 40.7010, -74.0000, 7, 58,
            arrive_by_ts=arrive_by,
            modes=["bus"],
            max_transfers=0,
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) >= 1, "should find at least one trip"
        for itin in itins:
            assert itin["arrive_at_ts"] <= arrive_by, (
                f"itinerary arrives at {itin['arrive_at_ts']} "
                f"but arrive_by_ts was {arrive_by}"
            )
    finally:
        _stop_engine(proc)


# ──────────────────────────────────────────────────────────────────
#  Gap 3 — Mode filter: bus-only excludes subway
# ──────────────────────────────────────────────────────────────────
#
#  Schedule has both a subway trip (E train, route_type 1) and a bus
#  trip (Q7, route_type 3) on overlapping geography.
#
#  Query: modes=["bus"]
#  Expected: no subway legs in results.
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap3_mode_filter_bus_only(tmp_path, engine_binary):
    db = tmp_path / "gap3.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("E",    None,  "8 Avenue Local",    "2850AD", 1),   # subway
        ("BUS3", "Q7",  "Queens Bus 7",      "F9A825", 3),   # bus
    ])
    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", [
        ("T_SUB", "E",    "WKD", "World Trade Center", 0),
        ("T_BUS", "BUS3", "WKD", "Queens Village",     0),
    ])
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SA", "Stop A", 40.7000, -74.0000),
        ("SB", "Stop B", 40.7010, -74.0000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", [
        ("T_SUB", "08:00:00", "08:00:00", "SA", 1),
        ("T_SUB", "08:08:00", "08:08:00", "SB", 2),
        ("T_BUS", "08:02:00", "08:02:00", "SA", 1),
        ("T_BUS", "08:12:00", "08:12:00", "SB", 2),
    ])
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        payload = _base_payload(
            40.7000, -74.0000, 40.7010, -74.0000, 7, 58,
            modes=["bus"],
            max_transfers=0,
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) >= 1, "should find the bus trip"
        for itin in itins:
            for leg in itin["legs"]:
                if leg["mode"] != "walk":
                    assert leg["mode"] == "bus", (
                        f"expected only bus legs, got mode={leg['mode']} "
                        f"route={leg['route_name']}"
                    )
    finally:
        _stop_engine(proc)


# ──────────────────────────────────────────────────────────────────
#  Gap 4 — num_itineraries cap is respected
# ──────────────────────────────────────────────────────────────────
#
#  Schedule has 5 different bus trips at staggered times.
#  Query: num_itineraries=2
#  Expected: at most 2 itineraries returned.
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap4_num_itineraries_cap(tmp_path, engine_binary):
    db = tmp_path / "gap4.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("BUS4", "B4", "Bus Four", "0078C6", 3),
    ])
    trips = []
    stimes = []
    for i in range(5):
        tid = f"T{i}"
        dep_min = 8 * 60 + i * 5  # 08:00, 08:05, 08:10, 08:15, 08:20
        arr_min = dep_min + 10
        dep_str = f"{dep_min // 60:02d}:{dep_min % 60:02d}:00"
        arr_str = f"{arr_min // 60:02d}:{arr_min % 60:02d}:00"
        trips.append((tid, "BUS4", "WKD", "Downtown", 0))
        stimes.append((tid, dep_str, dep_str, "SA", 1))
        stimes.append((tid, arr_str, arr_str, "SB", 2))

    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", trips)
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SA", "Stop A", 40.7000, -74.0000),
        ("SB", "Stop B", 40.7010, -74.0000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", stimes)
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        payload = _base_payload(
            40.7000, -74.0000, 40.7010, -74.0000, 7, 58,
            modes=["bus"],
            max_transfers=0,
            num_itineraries=2,
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) <= 2, (
            f"expected at most 2 itineraries, got {len(itins)}"
        )
    finally:
        _stop_engine(proc)


# ──────────────────────────────────────────────────────────────────
#  Gap 5 — priority=fewer_transfers prefers fewer transfers
# ──────────────────────────────────────────────────────────────────
#
#  Two paths from Origin → Destination:
#    Path A: 1 bus (direct but slower):  depart 08:00, arrive 08:25
#    Path B: 2 buses (transfer, faster): depart 08:00, arrive 08:20
#
#  Query: priority="fewer_transfers"
#  Expected: the direct (1-bus) path is ranked first.
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap5_priority_fewer_transfers(tmp_path, engine_binary):
    db = tmp_path / "gap5.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("EXPR",  "X1",  "Express Direct",  "FF0000", 3),   # direct
        ("LOCAL", "L1",  "Local Leg 1",     "00FF00", 3),   # leg 1
        ("FDR",   "F1",  "Feeder Leg 2",    "0000FF", 3),   # leg 2
    ])
    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", [
        ("T_EXPR",  "EXPR",  "WKD", "Downtown Direct",  0),
        ("T_LOC",   "LOCAL", "WKD", "Midtown",          0),
        ("T_FDR",   "FDR",   "WKD", "Downtown Transfer", 0),
    ])
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SO",  "Origin",     40.70000,  -74.00000),
        ("SM",  "Mid Point",  40.70070,  -74.00000),
        ("SD",  "Destination", 40.70900,  -74.00000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", [
        # Direct bus: 08:00 → 08:25 (slower but no transfer)
        ("T_EXPR", "08:00:00", "08:00:00", "SO", 1),
        ("T_EXPR", "08:25:00", "08:25:00", "SD", 2),
        # Local + feed: 08:00→08:08 + 08:12→08:20 (faster but 1 transfer)
        ("T_LOC",  "08:00:00", "08:00:00", "SO", 1),
        ("T_LOC",  "08:08:00", "08:08:00", "SM", 2),
        ("T_FDR",  "08:12:00", "08:12:00", "SM", 1),
        ("T_FDR",  "08:20:00", "08:20:00", "SD", 2),
    ])
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        payload = _base_payload(
            40.70000, -74.00000, 40.70900, -74.00000, 7, 58,
            modes=["bus"],
            max_transfers=2,
            priority="fewer_transfers",
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) >= 1, "should find at least one trip"
        # The first itinerary should have fewer transfers
        first = itins[0]
        assert first["transfer_count"] == 0, (
            f"with priority=fewer_transfers, first result should be direct "
            f"(0 transfers), got {first['transfer_count']}"
        )
    finally:
        _stop_engine(proc)


# ──────────────────────────────────────────────────────────────────
#  Gap 6 — CSA mode filter doesn't prune bus-only paths
# ──────────────────────────────────────────────────────────────────
#
#  Schedule has a subway trip (much faster) and a bus trip.
#  Query: modes=["bus"] (subway excluded)
#
#  Before fix: CSA would scan subway connections, produce tight
#  bounds, and RAPTOR would prune the slower bus path.
#  After fix: CSA skips subway, bus path survives RAPTOR pruning.
#

@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ not installed")
def test_gap6_csa_mode_filter_preserves_bus(tmp_path, engine_binary):
    db = tmp_path / "gap6.db"
    conn = _init_db(db)
    conn.executemany("INSERT INTO routes VALUES (?,?,?,?,?)", [
        ("E",     None,   "8 Avenue Local",  "2850AD", 1),   # subway (fast)
        ("BUS6", "Q42",  "Queens Bus 42",   "0078C6", 3),   # bus (slower)
    ])
    conn.executemany("INSERT INTO trips VALUES (?,?,?,?,?)", [
        ("T_SUB", "E",    "WKD", "WTC",    0),
        ("T_BUS", "BUS6", "WKD", "Jamaica", 0),
    ])
    conn.executemany("INSERT INTO stops VALUES (?,?,?,?)", [
        ("SA", "Stop A", 40.7000, -74.0000),
        ("SB", "Stop B", 40.7020, -74.0000),
    ])
    conn.executemany("INSERT INTO stop_times VALUES (?,?,?,?,?)", [
        # Subway: 08:00 → 08:05 (very fast)
        ("T_SUB", "08:00:00", "08:00:00", "SA", 1),
        ("T_SUB", "08:05:00", "08:05:00", "SB", 2),
        # Bus: 08:00 → 08:20 (slower, but the only allowed mode)
        ("T_BUS", "08:00:00", "08:00:00", "SA", 1),
        ("T_BUS", "08:20:00", "08:20:00", "SB", 2),
    ])
    conn.commit()
    conn.close()

    proc, port = _start_engine(engine_binary, db)
    try:
        payload = _base_payload(
            40.7000, -74.0000, 40.7020, -74.0000, 7, 58,
            modes=["bus"],
            max_transfers=0,
        )
        plan = _plan(port, payload)
        itins = plan["itineraries"]
        assert len(itins) >= 1, (
            "bus-only query should find the bus trip even though subway is faster"
        )
        for itin in itins:
            for leg in itin["legs"]:
                if leg["mode"] != "walk":
                    assert leg["mode"] == "bus", (
                        f"only bus expected, got {leg['mode']}"
                    )
    finally:
        _stop_engine(proc)
