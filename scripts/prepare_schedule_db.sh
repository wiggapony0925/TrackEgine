#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${TRACK_ENGINE_SCHEDULE_DB:-/app/data/transit_schedule.db}"
PREPARE_DB="${TRACK_ENGINE_PREPARE_DB:-1}"
BOOTSTRAP_DB_URL="${TRACK_ENGINE_BOOTSTRAP_DB_URL:-https://track-vkrr.onrender.com/engine/bootstrap/schedule-db.gz}"
BOOTSTRAP_TIMEOUT_S="${TRACK_ENGINE_BOOTSTRAP_TIMEOUT_S:-1800}"
BOOTSTRAP_RETRIES="${TRACK_ENGINE_BOOTSTRAP_RETRIES:-8}"

bootstrap_schedule_db() {
  if [[ -z "${BOOTSTRAP_DB_URL}" ]]; then
    return 1
  fi

  local db_dir
  db_dir="$(dirname "${DB_PATH}")"
  local tmp_gz="${DB_PATH}.download.gz"
  local tmp_db="${DB_PATH}.download"

  mkdir -p "${db_dir}"
  rm -f "${tmp_gz}" "${tmp_db}"

  echo "TrackEngine bootstrapping schedule DB from ${BOOTSTRAP_DB_URL}" >&2
  if ! curl \
    --fail \
    --location \
    --silent \
    --show-error \
    --retry "${BOOTSTRAP_RETRIES}" \
    --retry-all-errors \
    --retry-delay 2 \
    --connect-timeout 15 \
    --max-time "${BOOTSTRAP_TIMEOUT_S}" \
    --output "${tmp_gz}" \
    "${BOOTSTRAP_DB_URL}"; then
    echo "TrackEngine DB bootstrap failed: could not download ${BOOTSTRAP_DB_URL}" >&2
    rm -f "${tmp_gz}" "${tmp_db}"
    return 1
  fi

  if ! gunzip -c "${tmp_gz}" > "${tmp_db}"; then
    echo "TrackEngine DB bootstrap failed: could not decompress ${tmp_gz}" >&2
    rm -f "${tmp_gz}" "${tmp_db}"
    return 1
  fi

  if ! sqlite3 "${tmp_db}" "PRAGMA quick_check;" >/dev/null; then
    echo "TrackEngine DB bootstrap failed: downloaded DB failed quick_check" >&2
    rm -f "${tmp_gz}" "${tmp_db}"
    return 1
  fi

  mv "${tmp_db}" "${DB_PATH}"
  rm -f "${tmp_gz}"
  echo "TrackEngine bootstrapped schedule DB to ${DB_PATH}" >&2
  return 0
}

if [[ "${PREPARE_DB}" != "1" ]]; then
  exec trackengine "$@"
fi

if [[ ! -s "${DB_PATH}" ]]; then
  rm -f "${DB_PATH}"
  bootstrap_schedule_db || echo "TrackEngine DB prep continuing without bootstrap DB" >&2
fi

if [[ ! -f "${DB_PATH}" ]]; then
  echo "TrackEngine DB prep skipped: ${DB_PATH} does not exist" >&2
  exec trackengine "$@"
fi

if [[ ! -w "${DB_PATH}" ]]; then
  echo "TrackEngine DB prep skipped: ${DB_PATH} is not writable" >&2
  exec trackengine "$@"
fi

echo "Preparing TrackEngine schedule DB indexes for ${DB_PATH}" >&2
sqlite3 "${DB_PATH}" <<'SQL'
PRAGMA busy_timeout = 30000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
CREATE INDEX IF NOT EXISTS idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_arrival ON stop_times(arrival_time);
CREATE INDEX IF NOT EXISTS idx_stop_times_stop_dept ON stop_times(stop_id, departure_time);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip_seq ON stop_times(trip_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip_stop_seq ON stop_times(trip_id, stop_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_trips_service ON trips(service_id);
CREATE INDEX IF NOT EXISTS idx_calendar_date ON calendar_dates(date);
CREATE INDEX IF NOT EXISTS idx_calendar_service ON calendar(service_id);
CREATE INDEX IF NOT EXISTS idx_stops_name ON stops(stop_name);
ANALYZE;
SQL

exec trackengine "$@"
