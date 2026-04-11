"""Integration coverage for the TrackEngine startup bootstrap script."""

from __future__ import annotations

import gzip
import http.server
import os
import shutil
import socketserver
import sqlite3
import subprocess
import threading
from pathlib import Path

import pytest


def _build_schedule_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE stops (
            stop_id TEXT PRIMARY KEY,
            stop_name TEXT,
            stop_lat REAL,
            stop_lon REAL
        );
        CREATE TABLE stop_times (
            trip_id TEXT,
            arrival_time TEXT,
            departure_time TEXT,
            stop_id TEXT,
            stop_sequence INTEGER
        );
        CREATE TABLE trips (
            trip_id TEXT PRIMARY KEY,
            route_id TEXT,
            service_id TEXT
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
        """
    )
    conn.execute(
        "INSERT INTO stops VALUES ('STOP_A', 'Alpha', 40.0, -73.0)"
    )
    conn.execute(
        "INSERT INTO trips VALUES ('TRIP_1', 'ROUTE_1', 'WKD')"
    )
    conn.execute(
        """
        INSERT INTO calendar
        VALUES ('WKD', 1, 1, 1, 1, 1, 1, 1, '20240101', '20300101')
        """
    )
    conn.execute(
        "INSERT INTO stop_times VALUES ('TRIP_1', '08:00:00', '08:00:00', 'STOP_A', 1)"
    )
    conn.commit()
    conn.close()


class _SilentHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:  # noqa: A003
        del format, args


@pytest.mark.skipif(
    shutil.which("curl") is None or shutil.which("sqlite3") is None,
    reason="curl/sqlite3 not installed",
)
def test_prepare_schedule_db_script_bootstraps_missing_db(tmp_path: Path) -> None:
    source_db = tmp_path / "source.db"
    _build_schedule_db(source_db)

    payload_dir = tmp_path / "payload"
    payload_dir.mkdir()
    payload_path = payload_dir / "transit_schedule.db.gz"
    with source_db.open("rb") as source_stream:
        with gzip.open(payload_path, "wb", compresslevel=1) as gzip_stream:
            gzip_stream.write(source_stream.read())

    handler = lambda *args, **kwargs: _SilentHandler(  # noqa: E731
        *args,
        directory=str(payload_dir),
        **kwargs,
    )
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as server:
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            root = Path(__file__).resolve().parents[1]
            script = root / "scripts" / "prepare_schedule_db.sh"
            target_db = tmp_path / "data" / "transit_schedule.db"
            marker_path = tmp_path / "trackengine-called.txt"
            fake_trackengine = tmp_path / "trackengine"
            fake_trackengine.write_text(
                "#!/bin/sh\n"
                "printf 'called\\n' > \"$TRACKENGINE_MARKER\"\n"
            )
            fake_trackengine.chmod(0o755)

            env = {
                **os.environ,
                "PATH": f"{tmp_path}:{os.environ['PATH']}",
                "TRACK_ENGINE_SCHEDULE_DB": str(target_db),
                "TRACK_ENGINE_BOOTSTRAP_DB_URL": (
                    f"http://127.0.0.1:{port}/transit_schedule.db.gz"
                ),
                "TRACK_ENGINE_PREPARE_DB": "1",
                "TRACKENGINE_MARKER": str(marker_path),
            }

            subprocess.run(
                [str(script)],
                cwd=root.parent,
                check=True,
                env=env,
                capture_output=True,
                text=True,
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)

    assert target_db.exists()
    assert marker_path.read_text().strip() == "called"

    conn = sqlite3.connect(target_db)
    try:
        row = conn.execute("SELECT stop_name FROM stops").fetchone()
    finally:
        conn.close()

    assert row == ("Alpha",)
