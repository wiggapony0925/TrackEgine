#pragma once

// ── Connection Scan Algorithm (CSA) ──────────────────────────────
// Extremely fast single-criterion (earliest arrival) scanner.
// Used as a pre-filter to set tight upper bounds for RAPTOR,
// allowing aggressive pruning of the multi-criteria search.

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_set>
#include <vector>

#include "trackengine/schedule_store.hpp"

namespace trackengine {

class ConnectionScanner {
public:
    explicit ConnectionScanner(const ScheduleStore& store);

    /// Returns earliest arrival timestamp at every stop reachable
    /// from the given origin stops.  Unreachable stops have LLONG_MAX.
    /// @param route_allowed  Per-route bitmap; only connections on allowed
    ///                       routes are scanned (empty = allow all).
    std::vector<long long> scan(
        const std::vector<std::pair<uint32_t, long long>>& origin_arrivals,
        const std::unordered_set<std::string>& active_services,
        long long midnight_ts,
        const std::vector<bool>& route_allowed = {}
    ) const;

private:
    const ScheduleStore& store_;
};

}  // namespace trackengine
