// ── Connection Scan Algorithm — Implementation ───────────────────
// Single forward pass over all connections sorted by departure time.
// Extremely cache-friendly: scans a flat vector in order.  O(|C|)
// where |C| = number of connections.

#include "trackengine/csa.hpp"

#include <algorithm>
#include <limits>
#include <vector>

namespace trackengine {

ConnectionScanner::ConnectionScanner(const ScheduleStore& store)
    : store_(store) {}

std::vector<long long> ConnectionScanner::scan(
    const std::vector<std::pair<uint32_t, long long>>& origin_arrivals,
    const std::unordered_set<std::string>& active_services,
    long long midnight_ts,
    const std::vector<bool>& route_allowed
) const {
    const uint32_t n = store_.stop_count();
    const long long INF = std::numeric_limits<long long>::max();
    std::vector<long long> earliest(n, INF);

    // Seed origin stops
    for (const auto& [idx, arr] : origin_arrivals)
        if (idx < n) earliest[idx] = std::min(earliest[idx], arr);

    // Pre-compute active trip bitmap for fast lookup
    std::vector<bool> trip_ok(store_.trip_count(), false);
    for (uint32_t i = 0; i < store_.trip_count(); ++i)
        trip_ok[i] = active_services.count(store_.trip(i).service_id) > 0;

    // Track whether each trip has been "boarded" (i.e. we're riding it)
    // by recording the earliest boarding time.
    std::vector<long long> trip_boarded(store_.trip_count(), INF);

    // Single forward scan over all connections (sorted by departure_s)
    for (const auto& conn : store_.connections()) {
        if (!trip_ok[conn.trip_idx]) continue;
        // Mode filter: skip connections on disallowed routes
        if (!route_allowed.empty() && conn.route_idx < route_allowed.size()
            && !route_allowed[conn.route_idx]) continue;

        const long long dep_ts = midnight_ts + conn.departure_s;
        const long long arr_ts = midnight_ts + conn.arrival_s;

        // Can we board this connection's trip?
        // Either we're already on this trip, or we can reach the
        // departure stop before it leaves.
        const bool reachable =
            trip_boarded[conn.trip_idx] < INF ||
            earliest[conn.dep_stop_idx] <= dep_ts;

        if (!reachable) continue;

        // Mark trip as boarded
        if (trip_boarded[conn.trip_idx] >= INF)
            trip_boarded[conn.trip_idx] = dep_ts;

        // Relax arrival stop
        if (arr_ts < earliest[conn.arr_stop_idx]) {
            earliest[conn.arr_stop_idx] = arr_ts;
        }
    }

    return earliest;
}

}  // namespace trackengine
