#pragma once

// ── RAPTOR: Round-Based Public Transit Routing ───────────────────
// Multi-criteria optimization: finds Pareto-optimal itineraries
// trading off arrival time vs number of transfers.  Uses the
// in-memory ScheduleStore for cache-friendly pattern scanning and
// std::async for parallel route scanning within each round.

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "trackengine/engine_service.hpp"
#include "trackengine/schedule_store.hpp"

namespace trackengine {

// ── Journey pointer ──────────────────────────────────────────────
// Records *how* we arrived at a stop in a given RAPTOR round,
// enabling full path reconstruction after all rounds complete.

struct TransitJourney {
    uint32_t pattern_idx  = UINT32_MAX;
    uint32_t trip_offset  = UINT32_MAX;
    uint32_t board_pos    = UINT32_MAX;   // index in pattern.stop_indices
    uint32_t alight_pos   = UINT32_MAX;   // index in pattern.stop_indices
};

struct TransferJourney {
    uint32_t from_stop_idx = UINT32_MAX;
    double walk_meters     = 0.0;
    int walk_seconds       = 0;
};

// ── Router ───────────────────────────────────────────────────────

class RaptorRouter {
public:
    explicit RaptorRouter(const ScheduleStore& store);

    /// Run RAPTOR and return Pareto-optimal itineraries.
    std::vector<Itinerary> route(
        const PlanRequest& request,
        const std::vector<StopCandidate>& origin_candidates,
        const std::vector<StopCandidate>& dest_candidates,
        const std::vector<std::string>& active_services,
        const std::vector<long long>& csa_bounds  // per-stop earliest arrival (or empty)
    ) const;

private:
    const ScheduleStore& store_;
};

}  // namespace trackengine
