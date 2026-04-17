#pragma once

// ── In-Memory Schedule Store ─────────────────────────────────────
// Loads all GTFS tables from SQLite into contiguous std::vectors
// at startup.  All routing queries use these flat arrays instead of
// per-query SQLite I/O, maximising CPU-cache efficiency.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace trackengine {

// ── Core data types ──────────────────────────────────────────────

struct MemStop {
    uint32_t idx = 0;
    std::string stop_id;
    std::string stop_name;
    double lat = 0.0;
    double lon = 0.0;
};

struct MemRoute {
    uint32_t idx = 0;
    std::string route_id;
    std::string route_name;
    std::string route_long_name;
    std::optional<std::string> color_hex;
    std::optional<int> route_type;
    std::string mode;  // "subway", "bus", "lirr", "mnr"
};

struct MemTrip {
    uint32_t idx = 0;
    std::string trip_id;
    uint32_t route_idx = 0;
    std::string service_id;
    std::optional<std::string> headsign;
};

// ── Route pattern ────────────────────────────────────────────────
// A pattern is a unique ordered sequence of stops served by one or
// more trips on the same GTFS route.  RAPTOR scans patterns for
// cache-efficient downstream enumeration.

struct PatternTrip {
    uint32_t trip_idx = 0;
    // Flat interleaved array: [dep_s_0, arr_s_0, dep_s_1, arr_s_1, …]
    // size = 2 * number_of_stops_in_pattern
    std::vector<int> times;
};

struct RoutePattern {
    uint32_t idx = 0;
    uint32_t route_idx = 0;
    std::vector<uint32_t> stop_indices;    // ordered stops
    std::vector<PatternTrip> trips;        // sorted by dep time at first stop
};

// ── CSA connection ───────────────────────────────────────────────
// One hop on one trip between consecutive pattern stops, sorted
// globally by departure time for the Connection Scan Algorithm.

struct MemConnection {
    uint32_t dep_stop_idx = 0;
    uint32_t arr_stop_idx = 0;
    uint32_t trip_idx = 0;
    uint32_t route_idx = 0;
    int departure_s = 0;   // seconds since service-day midnight
    int arrival_s = 0;
};

// ── Schedule Store ───────────────────────────────────────────────

class ScheduleStore {
public:
    /// Build the in-memory store from a SQLite schedule database.
    explicit ScheduleStore(const std::string& db_path);

    // ── Stop access ──────────────────────────────────────────────
    const MemStop& stop(uint32_t idx) const { return stops_[idx]; }
    std::optional<uint32_t> stop_idx(const std::string& stop_id) const;
    uint32_t stop_count() const { return static_cast<uint32_t>(stops_.size()); }
    const std::vector<MemStop>& all_stops() const { return stops_; }
    bool stop_has_departures(uint32_t idx) const { return stops_with_departures_.count(idx); }

    // ── Route access ─────────────────────────────────────────────
    const MemRoute& route(uint32_t idx) const { return routes_[idx]; }
    std::optional<uint32_t> route_idx(const std::string& route_id) const;
    uint32_t route_count() const { return static_cast<uint32_t>(routes_.size()); }

    // ── Trip access ──────────────────────────────────────────────
    const MemTrip& trip(uint32_t idx) const { return trips_[idx]; }
    uint32_t trip_count() const { return static_cast<uint32_t>(trips_.size()); }

    // ── Pattern access (for RAPTOR) ──────────────────────────────
    const std::vector<RoutePattern>& patterns() const { return patterns_; }
    /// Patterns serving a stop: [(pattern_idx, position_in_pattern), …]
    const std::vector<std::pair<uint32_t, uint32_t>>& patterns_at_stop(uint32_t stop_idx) const;

    // ── CSA connections (sorted by departure_s) ──────────────────
    const std::vector<MemConnection>& connections() const { return connections_; }

    // ── Spatial queries ──────────────────────────────────────────
    std::vector<std::pair<uint32_t, double>> nearby_stops(
        double lat, double lon, int radius_m, std::size_t limit) const;

    // ── Calendar ─────────────────────────────────────────────────
    std::vector<std::string> active_services(int yyyymmdd, int weekday) const;

    bool loaded() const { return loaded_; }

private:
    struct CalEntry {
        std::string service_id;
        bool days[7] = {};   // 0=mon … 6=sun
        int start_date = 0;  // yyyymmdd
        int end_date = 0;
    };
    struct CalDateEntry {
        std::string service_id;
        int date = 0;
        int exception_type = 0;
    };

    bool loaded_ = false;

    std::vector<MemStop> stops_;
    std::vector<MemRoute> routes_;
    std::vector<MemTrip> trips_;
    std::vector<RoutePattern> patterns_;
    std::vector<MemConnection> connections_;
    std::vector<CalEntry> calendar_;
    std::vector<CalDateEntry> calendar_dates_;

    std::unordered_map<std::string, uint32_t> stop_id_map_;
    std::unordered_map<std::string, uint32_t> route_id_map_;
    std::unordered_map<std::string, uint32_t> trip_id_map_;
    std::unordered_set<uint32_t> stops_with_departures_;

    // stop_idx → [(pattern_idx, position_in_pattern)]
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> stop_pattern_idx_;

    // Stops sorted by latitude for fast bounding-box scans.
    std::vector<uint32_t> stops_by_lat_;

    static const std::vector<std::pair<uint32_t, uint32_t>> kEmptyPatterns;
};

}  // namespace trackengine
