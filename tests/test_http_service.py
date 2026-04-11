"""Standalone C++ TrackEngine service integration tests."""

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


def _build_schedule_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
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
    )
    conn.executemany(
        "INSERT INTO routes VALUES (?, ?, ?, ?, ?)",
        [
            ("R1", "Q7", "Queens Bus 7", "F9A825", 3),
            ("R2", "Q37", "Queens Bus 37", "59ADEB", 3),
            ("R3", "E", "8 Avenue Local", "2850AD", 1),
        ],
    )
    conn.executemany(
        "INSERT INTO trips VALUES (?, ?, ?, ?, ?)",
        [
            ("T1", "R1", "WKD", "Queens Village", 0),
            ("T2", "R2", "WKD", "Kew Gardens", 0),
            ("T3", "R3", "WKD", "Manhattan", 0),
        ],
    )
    conn.executemany(
        "INSERT INTO stops VALUES (?, ?, ?, ?)",
        [
            ("STOP_A", "Origin Stop", 40.00000, -73.00000),
            ("STOP_B", "Transfer One", 40.00070, -73.00000),
            ("STOP_C", "Transfer Two", 40.00140, -73.00000),
            ("STOP_D", "Destination Stop", 40.01000, -73.00000),
        ],
    )
    conn.executemany(
        "INSERT INTO stop_times VALUES (?, ?, ?, ?, ?)",
        [
            ("T1", "08:00:00", "08:00:00", "STOP_A", 1),
            ("T1", "08:05:00", "08:05:00", "STOP_B", 2),
            ("T2", "08:09:00", "08:09:00", "STOP_B", 1),
            ("T2", "08:14:00", "08:14:00", "STOP_C", 2),
            ("T3", "08:18:00", "08:18:00", "STOP_C", 1),
            ("T3", "08:24:00", "08:24:00", "STOP_D", 2),
        ],
    )
    conn.execute(
        """
        INSERT INTO calendar
        VALUES ('WKD', 1, 1, 1, 1, 1, 1, 1, '20240101', '20300101')
        """
    )
    conn.commit()
    conn.close()


def _build_route_collision_schedule_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
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
    )
    conn.execute(
        "INSERT INTO routes VALUES (?, ?, ?, ?, ?)",
        ("7", None, "Far Rockaway Branch", "6E3219", 2),
    )
    conn.execute(
        "INSERT INTO trips VALUES (?, ?, ?, ?, ?)",
        ("T7", "7", "WKD", "Flushing-Main St", 0),
    )
    conn.executemany(
        "INSERT INTO stops VALUES (?, ?, ?, ?)",
        [
            ("718S", "74 St-Broadway", 40.74680, -73.89140),
            ("615S", "61 St-Woodside", 40.74590, -73.90220),
        ],
    )
    conn.executemany(
        "INSERT INTO stop_times VALUES (?, ?, ?, ?, ?)",
        [
            ("T7", "08:00:00", "08:00:00", "718S", 1),
            ("T7", "08:04:00", "08:04:00", "615S", 2),
        ],
    )
    conn.execute(
        """
        INSERT INTO calendar
        VALUES ('WKD', 1, 1, 1, 1, 1, 1, 1, '20240101', '20300101')
        """
    )
    conn.commit()
    conn.close()


def _timestamp(hour: int, minute: int) -> int:
    now = datetime.now(NY_TZ)
    dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return int(dt.timestamp())


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ compiler not installed")
def test_cpp_http_service_plans_two_transfer_trip(tmp_path: Path) -> None:
    schedule_db = tmp_path / "schedule.db"
    _build_schedule_db(schedule_db)

    root = Path(__file__).resolve().parents[1]
    build = subprocess.run(
        [str(root / "scripts" / "build.sh")],
        cwd=root.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    binary = build.stdout.strip().splitlines()[-1]
    port = _free_port()

    process = subprocess.Popen(
        [binary],
        cwd=root.parent,
        env={
            **os.environ,
            "PORT": str(port),
            "TRACK_ENGINE_SCHEDULE_DB": str(schedule_db),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.time() + 20
        health_url = f"http://127.0.0.1:{port}/health"
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as response:
                    payload = json.loads(response.read().decode())
                    if payload["ready"]:
                        assert payload["missing_indexes"] == []
                        break
            except Exception:
                time.sleep(0.2)
        else:
            output = process.stdout.read() if process.stdout else ""
            raise AssertionError(f"engine never became healthy\n{output}")

        query_ts = _timestamp(7, 58)
        service_day = datetime.fromtimestamp(query_ts, NY_TZ).date()
        midnight = datetime.combine(service_day, datetime.min.time(), tzinfo=NY_TZ)
        payload = {
            "origin": {"label": "Home", "lat": 40.00001, "lon": -73.00000},
            "destination": {"label": "Work", "lat": 40.01001, "lon": -73.00000},
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
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/plan",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            plan = json.loads(response.read().decode())

        assert plan["itineraries"]
        assert plan["itineraries"][0]["transfer_count"] == 2
        route_ids = [
            leg["route_id"]
            for leg in plan["itineraries"][0]["legs"]
            if leg["mode"] != "walk"
        ]
        assert route_ids == ["R1", "R2", "R3"]

        go_request = urllib.request.Request(
            f"http://127.0.0.1:{port}/go",
            data=json.dumps({**payload, "now_ts": query_ts}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(go_request, timeout=20) as response:
            go_payload = json.loads(response.read().decode())

        assert go_payload["primary_trip"] is not None
        transit_chip_labels = [
            chip["label"]
            for chip in go_payload["primary_trip"]["route_chips"]
            if chip["kind"] == "transit"
        ]
        assert transit_chip_labels == ["Q7", "Q37", "E"]
        assert len(go_payload["primary_trip"]["transfers"]) == 2
        assert go_payload["primary_trip"]["next_action"]["title"].startswith("Leave")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.mark.skipif(shutil.which("c++") is None, reason="c++ compiler not installed")
def test_cpp_http_service_normalizes_subway_route_collision(tmp_path: Path) -> None:
    schedule_db = tmp_path / "collision.db"
    _build_route_collision_schedule_db(schedule_db)

    root = Path(__file__).resolve().parents[1]
    build = subprocess.run(
        [str(root / "scripts" / "build.sh")],
        cwd=root.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    binary = build.stdout.strip().splitlines()[-1]
    port = _free_port()

    process = subprocess.Popen(
        [binary],
        cwd=root.parent,
        env={
            **os.environ,
            "PORT": str(port),
            "TRACK_ENGINE_SCHEDULE_DB": str(schedule_db),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.time() + 20
        health_url = f"http://127.0.0.1:{port}/health"
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as response:
                    payload = json.loads(response.read().decode())
                    if payload["ready"]:
                        assert payload["missing_indexes"] == []
                        break
            except Exception:
                time.sleep(0.2)
        else:
            output = process.stdout.read() if process.stdout else ""
            raise AssertionError(f"engine never became healthy\n{output}")

        query_ts = _timestamp(7, 59)
        service_day = datetime.fromtimestamp(query_ts, NY_TZ).date()
        midnight = datetime.combine(service_day, datetime.min.time(), tzinfo=NY_TZ)
        payload = {
            "origin": {"label": "Origin", "lat": 40.74680, "lon": -73.89140},
            "destination": {"label": "Destination", "lat": 40.74590, "lon": -73.90220},
            "depart_at_ts": query_ts,
            "query_ts": query_ts,
            "service_day_midnight_ts": int(midnight.timestamp()),
            "service_day_yyyymmdd": int(service_day.strftime("%Y%m%d")),
            "service_weekday": service_day.weekday(),
            "max_transfers": 0,
            "max_origin_walk_m": 30,
            "max_destination_walk_m": 30,
            "max_transfer_walk_m": 30,
            "search_window_minutes": 60,
            "num_itineraries": 1,
            "modes": ["subway", "lirr", "mnr"],
        }
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/plan",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            plan = json.loads(response.read().decode())

        assert plan["itineraries"]
        transit_legs = [
            leg for leg in plan["itineraries"][0]["legs"] if leg["mode"] != "walk"
        ]
        assert transit_legs[0]["route_name"] == "7"
        assert transit_legs[0]["mode"] == "subway"
        assert transit_legs[0]["color_hex"] == "#B933AD"
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
