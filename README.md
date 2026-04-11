# TrackEngine

TrackEngine is the standalone C++ routing service for the Track app.

It is designed to run as its own Docker service and answer trip-planning
requests over HTTP:

- `GET /health`
- `POST /plan`
- `POST /go`

The Track backend still owns user-specific state such as saved places,
saved trips, recents, calendar events, and recommendation scoring. The
hot routing path itself now lives in the C++ service.

## Current Capabilities

- SQLite-backed GTFS routing
- Access and egress walking
- Direct and transfer itineraries
- Route/leg metadata for frontend trip cards
- Go-mode trip shaping in C++ (`route_chips`, `steps`, `transfers`, `next_action`)
- Standalone HTTP service for backend-to-engine calls

## Build

```bash
./scripts/build.sh
```

The binary is written to:

```text
build/bin/trackengine
```

## Run Locally

```bash
PORT=8080 \
TRACK_ENGINE_SCHEDULE_DB=/absolute/path/to/transit_schedule.db \
./build/bin/trackengine
```

Then hit:

```bash
curl http://127.0.0.1:8080/health
```

The runtime image now runs an idempotent DB-prepare step before starting
the engine. If the mounted schedule DB is writable, it will create the
hot-path indexes the router depends on and run `ANALYZE`.

## Request Shape

`POST /plan` expects JSON like:

```json
{
  "origin": {
    "label": "Home",
    "lat": 40.7021,
    "lon": -73.8019
  },
  "destination": {
    "label": "Penn Station",
    "lat": 40.7506,
    "lon": -73.9935
  },
  "depart_at_ts": 1775908800,
  "query_ts": 1775908800,
  "service_day_yyyymmdd": 20260411,
  "service_weekday": 5,
  "service_day_midnight_ts": 1775889600,
  "max_transfers": 1,
  "max_origin_walk_m": 1500,
  "max_destination_walk_m": 1500,
  "max_transfer_walk_m": 250,
  "search_window_minutes": 180,
  "num_itineraries": 3,
  "modes": ["subway", "bus", "lirr", "mnr"]
}
```

The backend computes `query_ts` and the service-day fields before calling
the engine so the C++ service can stay focused on route search.

`POST /go` accepts the same request shape, plus optional `now_ts`, and
returns frontend-ready trip cards:

- route chips
- step list
- transfer blocks
- next action / leave-now state
- alternative trips

## Docker

Build the standalone container:

```bash
docker build -t trackengine-cpp .
```

Run it:

```bash
docker run --rm -p 8080:8080 \
  -e TRACK_ENGINE_SCHEDULE_DB=/app/data/transit_schedule.db \
  -v /absolute/path/to/data:/app/data \
  trackengine-cpp
```

## Tests

Run the standalone C++ service integration test:

```bash
python3 -m pytest TrackEngine/tests/test_http_service.py -q
```

Run the backend engine route tests:

```bash
python3 -m pytest TrackBackend/tests/test_engine_api.py -q
```

## Deployment Notes

- The backend should call this service through `TRACK_ENGINE_URL`.
- The engine service needs its own `transit_schedule.db` on disk.
- In Render, give the engine its own persistent disk mounted at `/app/data`.
- Seed that disk with the same schedule DB artifact the backend uses before
  switching production routing traffic to the standalone engine.
- Keep `TRACK_ENGINE_PREPARE_DB=1` so the service self-heals missing indexes
  on existing schedule DB artifacts before serving traffic.
- Check `/health` for `missing_indexes`; if that list is non-empty, routing
  performance will be degraded even if the DB file exists.
- This folder now includes an engine-only `render.yaml` so the standalone
  `TrackEgine` repo can be deployed directly on Render.
