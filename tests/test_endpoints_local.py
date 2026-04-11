#!/usr/bin/env python3
"""Quick test of TrackEngine /plan and /go endpoints."""

import json
import sys
import urllib.request
from datetime import datetime, time as time_value
from zoneinfo import ZoneInfo

ENGINE_URL = "http://127.0.0.1:8091"
NY = ZoneInfo("America/New_York")

now = datetime.now(NY)
sd = now.date()
midnight = datetime.combine(sd, time_value.min, tzinfo=NY)
query_ts = int(now.timestamp())

# Times Sq to Grand Central (classic NYC test)
plan_payload = {
    "origin": {"label": "Times Square", "lat": 40.7580, "lon": -73.9855},
    "destination": {"label": "Grand Central", "lat": 40.7527, "lon": -73.9772},
    "depart_at_ts": query_ts,
    "query_ts": query_ts,
    "service_day_midnight_ts": int(midnight.timestamp()),
    "service_day_yyyymmdd": int(sd.strftime("%Y%m%d")),
    "service_weekday": sd.weekday(),
    "max_transfers": 1,
    "max_origin_walk_m": 800,
    "max_destination_walk_m": 800,
    "max_transfer_walk_m": 400,
    "search_window_minutes": 60,
    "num_itineraries": 3,
    "modes": ["subway", "bus"],
}

print("=" * 60)
print("TEST: /health")
print("=" * 60)
try:
    with urllib.request.urlopen(f"{ENGINE_URL}/health", timeout=5) as resp:
        health = json.loads(resp.read())
        print(json.dumps(health, indent=2))
        assert health["ready"], "Engine not ready!"
        print("PASS: engine is ready\n")
except Exception as e:
    print(f"FAIL: {e}")
    sys.exit(1)

print("=" * 60)
print("TEST: /plan  (Times Square -> Grand Central)")
print("=" * 60)
try:
    req = urllib.request.Request(
        f"{ENGINE_URL}/plan",
        data=json.dumps(plan_payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        print(f"Engine version: {data.get('engine_version')}")
        itin = data.get("itineraries", [])
        print(f"Itineraries found: {len(itin)}")
        for i, it in enumerate(itin):
            print(f"  [{i}] {it['summary']}")
            print(f"      duration={it['total_duration_s']}s  transfers={it['transfer_count']}")
            for leg in it["legs"]:
                print(f"      {leg['mode']:7s} {leg['route_name']:5s}  "
                      f"{leg['board_stop_name']} -> {leg['alight_stop_name']}  "
                      f"{leg['duration_s']}s")
        if itin:
            print("PASS: got itineraries\n")
        else:
            print("WARN: no itineraries (may be no service at this hour)\n")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"FAIL: HTTP {e.code} — {body}")
    sys.exit(1)

print("=" * 60)
print("TEST: /go  (Times Square -> Grand Central)")
print("=" * 60)
go_payload = {**plan_payload, "now_ts": query_ts}
try:
    req = urllib.request.Request(
        f"{ENGINE_URL}/go",
        data=json.dumps(go_payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        print(f"Engine version: {data.get('engine_version')}")
        print(f"Session kind: {data.get('session_kind')}")
        primary = data.get("primary_trip")
        if primary:
            it = primary["itinerary"]
            print(f"Primary trip: {it['summary']}")
            print(f"  Duration: {primary['duration_label']}")
            print(f"  Leave: {primary['leave_label']}")
            print(f"  Arrive: {primary['arrive_label']}")
            print(f"  Next action: {primary['next_action']}")
            chips = primary.get("route_chips", [])
            print(f"  Route chips: {[c['label'] for c in chips]}")
            steps = primary.get("steps", [])
            print(f"  Steps: {len(steps)}")
            for step in steps:
                print(f"    [{step['kind']}] {step['title']}  |  {step.get('subtitle','')}")
        else:
            print("WARN: no primary trip (may be no service at this hour)")
        alts = data.get("alternatives", [])
        print(f"Alternatives: {len(alts)}")
        for i, alt in enumerate(alts):
            print(f"  [{i}] {alt['itinerary']['summary']}  {alt['duration_label']}")
        print("PASS: /go endpoint works\n")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"FAIL: HTTP {e.code} — {body}")
    sys.exit(1)

print("=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
