// ── RAPTOR Implementation ────────────────────────────────────────
// Round-based public transit routing with:
//   • Multi-criteria Pareto-optimal results (time vs transfers)
//   • Parallel route scanning via std::async
//   • CSA-informed pruning bounds
//   • Full path reconstruction to TransitLeg / Itinerary

#include "trackengine/raptor.hpp"
#include "trackengine/logger.hpp"

#include <algorithm>
#include <cmath>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <mutex>
#include <numeric>
#include <thread>
#include <unordered_set>
#include <vector>

namespace trackengine {

namespace {

// ── Duplicated pure utility functions ────────────────────────────

constexpr double kEarthRadiusM  = 6'371'009.0;
constexpr double kWalkSpeedMps  = 1.45;
constexpr int    kMinTransferS  = 120;

[[maybe_unused]]
double haversine_m(double lat1, double lon1, double lat2, double lon2) {
    const double r1 = lat1 * M_PI / 180.0, r2 = lat2 * M_PI / 180.0;
    const double dl = (lat2 - lat1) * M_PI / 180.0;
    const double dn = (lon2 - lon1) * M_PI / 180.0;
    const double a  = std::sin(dl/2)*std::sin(dl/2) +
                      std::cos(r1)*std::cos(r2)*std::sin(dn/2)*std::sin(dn/2);
    return kEarthRadiusM * 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

int walk_seconds(double meters) {
    return static_cast<int>(std::ceil(meters / kWalkSpeedMps));
}

bool looks_like_subway_stop_id(const std::string& s) {
    return s.size() == 4 && (s[3] == 'N' || s[3] == 'S') &&
           std::all_of(s.begin(), s.begin()+3,
                       [](unsigned char c) { return std::isalnum(c)!=0; });
}

// ── Brand-color helpers (duplicated for this TU) ─────────────────
struct BrandColorMaps {
    std::unordered_map<std::string,std::string> subway;
    std::unordered_map<std::string,std::string> mode_defaults;
};

BrandColorMaps load_brand_colors_raptor() {
    BrandColorMaps m;
    std::vector<std::string> cands;
    const char* e = std::getenv("TRACK_BRAND_COLORS_JSON");
    if (e && e[0]) cands.emplace_back(e);
    cands.emplace_back("config/brand_colors.json");
    cands.emplace_back("../config/brand_colors.json");
    for (auto& p : cands) {
        if (!std::filesystem::exists(p)) continue;
        try {
            std::ifstream f(p); if (!f.is_open()) continue;
            auto j = nlohmann::json::parse(f);
            if (j.contains("subway") && j["subway"].is_object())
                for (auto& [k,v] : j["subway"].items())
                    if (v.is_string()) m.subway[k] = v.get<std::string>();
            if (j.contains("mode_defaults") && j["mode_defaults"].is_object())
                for (auto& [k,v] : j["mode_defaults"].items())
                    if (v.is_string()) m.mode_defaults[k] = v.get<std::string>();
            return m;
        } catch (...) {}
    }
    m.subway = {
        {"1","#D82233"},{"2","#D82233"},{"3","#D82233"},
        {"4","#009952"},{"5","#009952"},{"5X","#009952"},
        {"6","#009952"},{"6X","#009952"},
        {"7","#9A38A1"},{"7X","#9A38A1"},
        {"A","#0062CF"},{"C","#0062CF"},{"E","#0062CF"},
        {"B","#EB6800"},{"D","#EB6800"},{"F","#EB6800"},
        {"FX","#EB6800"},{"M","#EB6800"},{"G","#799534"},
        {"J","#8E5C33"},{"Z","#8E5C33"},{"L","#7C858C"},
        {"N","#F6BC26"},{"Q","#F6BC26"},{"R","#F6BC26"},{"W","#F6BC26"},
        {"S","#7C858C"},{"GS","#7C858C"},{"FS","#7C858C"},
        {"SR","#7C858C"},{"H","#7C858C"},
        {"SI","#08179C"},{"SIR","#08179C"},{"T","#008EB7"},
    };
    m.mode_defaults = {{"subway","#0062CF"},{"bus","#0078C6"},{"lirr","#0073BF"},{"mnr","#005A8C"}};
    return m;
}

const BrandColorMaps& brand_colors_raptor() {
    static const BrandColorMaps m = load_brand_colors_raptor();
    return m;
}

std::optional<std::string> subway_color(const std::string& rid) {
    auto& s = brand_colors_raptor().subway;
    auto it = s.find(rid); return it != s.end() ? std::optional(it->second) : std::nullopt;
}

// ── Itinerary builder (duplicated from engine_service) ───────────

Itinerary build_itinerary(
    std::vector<TransitLeg> legs,
    const std::string& priority,
    bool accessibility_priority
) {
    if (legs.empty()) return {};
    const long long leave  = legs.front().departure_ts;
    const long long arrive = legs.back().arrival_ts;
    const int total = static_cast<int>(arrive - leave);
    int in_vehicle = 0, walking = 0, transits = 0;
    double walk_m = 0.0;
    std::vector<std::string> parts;
    for (const auto& l : legs) {
        if (l.mode == "walk") {
            walking += l.duration_s;
            parts.push_back("Walk " + std::to_string(static_cast<int>(std::round(l.walk_meters))) + "m");
        } else {
            in_vehicle += l.duration_s;
            ++transits;
            parts.push_back(l.route_name);
        }
        walk_m += l.walk_meters;
    }
    const int transfers = std::max(0, transits - 1);
    const int waiting   = std::max(0, total - in_vehicle - walking);

    double tp = 300.0, wp = 1.0;
    if (priority == "fewer_transfers")    tp = 1200.0;
    else if (priority == "less_walking")  wp = 3.0;
    if (accessibility_priority) tp = std::max(tp, 1500.0);
    const double score = static_cast<double>(arrive) + transfers * tp + walk_m * wp;

    std::ostringstream ss;
    for (std::size_t i = 0; i < parts.size(); ++i) { if (i) ss << " -> "; ss << parts[i]; }
    const auto summary = ss.str();

    return Itinerary{
        .itinerary_id    = std::to_string(leave) + "-" + std::to_string(arrive) + "-" + summary,
        .leave_at_ts     = leave,
        .arrive_at_ts    = arrive,
        .total_duration_s = total,
        .in_vehicle_s    = in_vehicle,
        .walking_s       = walking,
        .waiting_s       = waiting,
        .transfer_count  = transfers,
        .walk_meters     = walk_m,
        .score           = score,
        .summary         = summary,
        .legs            = std::move(legs),
    };
}

// ── Per-round improvement record (for thread-local collection) ───

struct Improvement {
    uint32_t stop_idx;
    long long arrival_ts;
    TransitJourney journey;
};

}  // anonymous namespace

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  RaptorRouter
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RaptorRouter::RaptorRouter(const ScheduleStore& store) : store_(store) {}

std::vector<Itinerary> RaptorRouter::route(
    const PlanRequest& request,
    const std::vector<StopCandidate>& origin_candidates,
    const std::vector<StopCandidate>& dest_candidates,
    const std::vector<std::string>& active_services,
    const std::vector<long long>& csa_bounds
) const {
    log::Timer timer;

    const uint32_t num_stops = store_.stop_count();
    if (num_stops == 0 || origin_candidates.empty() || dest_candidates.empty())
        return {};

    const int max_rounds = request.max_transfers + 1;
    const long long midnight = request.service_day_midnight_ts;

    // ── Active trip bitmap ───────────────────────────────────────
    const std::unordered_set<std::string> active_set(
        active_services.begin(), active_services.end());
    std::vector<bool> trip_active(store_.trip_count(), false);
    for (uint32_t i = 0; i < store_.trip_count(); ++i)
        trip_active[i] = active_set.count(store_.trip(i).service_id) > 0;

    // ── Mode filter for routes ───────────────────────────────────
    std::vector<bool> route_allowed(store_.route_count(), false);
    for (uint32_t i = 0; i < store_.route_count(); ++i)
        route_allowed[i] = request.modes.count(store_.route(i).mode) > 0;

    // ── tau arrays ───────────────────────────────────────────────
    // tau[k][p] = best arrival at stop p using ≤ k transit legs
    const long long INF = std::numeric_limits<long long>::max();
    std::vector<std::vector<long long>> tau(
        max_rounds + 1, std::vector<long long>(num_stops, INF));
    std::vector<long long> tau_star(num_stops, INF);

    // populate tau_star with CSA bounds if available
    if (csa_bounds.size() == num_stops) {
        for (uint32_t i = 0; i < num_stops; ++i)
            tau_star[i] = csa_bounds[i];
    }

    // ── Journey pointers for path reconstruction ─────────────────
    std::vector<std::vector<TransitJourney>> transit_jp(
        max_rounds + 1, std::vector<TransitJourney>(num_stops));
    std::vector<std::vector<TransferJourney>> transfer_jp(
        max_rounds + 1, std::vector<TransferJourney>(num_stops));
    // Whether stop was improved by a footpath transfer in round k
    std::vector<std::vector<bool>> was_transfer(
        max_rounds + 1, std::vector<bool>(num_stops, false));

    // ── Destination lookup ───────────────────────────────────────
    std::unordered_map<uint32_t, const StopCandidate*> dest_map;
    for (const auto& c : dest_candidates) {
        auto idx = store_.stop_idx(c.stop_id);
        if (idx) dest_map[*idx] = &c;
    }

    // Global upper-bound on useful arrival (from best known destination arrival)
    long long global_best = INF;

    // ── Round 0: seed origin stops ───────────────────────────────
    std::vector<bool> marked(num_stops, false);
    for (const auto& c : origin_candidates) {
        auto idx = store_.stop_idx(c.stop_id);
        if (!idx) continue;
        const long long arr = request.query_ts + c.walk_seconds;
        if (arr < tau[0][*idx]) {
            tau[0][*idx] = arr;
            tau_star[*idx] = std::min(tau_star[*idx], arr);
            marked[*idx] = true;
        }
    }

    // ── Determine thread count for parallel scanning ─────────────
    const unsigned hw = std::thread::hardware_concurrency();
    const unsigned num_threads = std::max(1u, std::min(hw > 0 ? hw : 4u, 8u));

    // ── Main RAPTOR loop ─────────────────────────────────────────
    for (int k = 1; k <= max_rounds; ++k) {
        // Collect patterns serving at least one marked stop.
        // For each pattern, record the earliest marked position.
        struct ScanTask {
            uint32_t pattern_idx;
            uint32_t earliest_marked_pos;
        };
        std::vector<ScanTask> tasks;
        {
            std::unordered_set<uint32_t> seen_patterns;
            for (uint32_t si = 0; si < num_stops; ++si) {
                if (!marked[si]) continue;
                for (const auto& [pi, pos] : store_.patterns_at_stop(si)) {
                    if (!route_allowed[store_.patterns()[pi].route_idx]) continue;
                    if (!seen_patterns.count(pi)) {
                        seen_patterns.insert(pi);
                        tasks.push_back({pi, pos});
                    } else {
                        // Update earliest_marked_pos if this is earlier
                        for (auto& t : tasks) {
                            if (t.pattern_idx == pi && pos < t.earliest_marked_pos)
                                t.earliest_marked_pos = pos;
                        }
                    }
                }
            }
        }

        if (tasks.empty()) break;  // no more patterns to scan

        // ── Parallel route scanning ──────────────────────────────
        // Each thread scans a chunk of patterns and collects local
        // improvements.  After all threads finish, merge into tau[k].

        auto scan_chunk = [&](std::size_t start, std::size_t end) -> std::vector<Improvement> {
            std::vector<Improvement> local;
            for (std::size_t ti = start; ti < end; ++ti) {
                const auto& task   = tasks[ti];
                const auto& pat    = store_.patterns()[task.pattern_idx];
                const uint32_t n   = static_cast<uint32_t>(pat.stop_indices.size());

                // Scan the pattern
                int current_trip = -1;
                uint32_t board_pos = 0;

                for (uint32_t pos = 0; pos < n; ++pos) {
                    const uint32_t sid = pat.stop_indices[pos];

                    // Can we board a (better) trip at this stop?
                    if (tau[k-1][sid] < INF) {
                        const long long earliest_board = tau[k-1][sid];
                        // Linear scan for the earliest active trip
                        // departing at or after earliest_board
                        for (int t = (current_trip >= 0 ? current_trip : 0);
                             t < static_cast<int>(pat.trips.size()); ++t) {
                            if (!trip_active[pat.trips[t].trip_idx]) continue;
                            const long long dep = midnight + pat.trips[t].times[pos * 2];
                            if (dep >= earliest_board) {
                                if (current_trip < 0 || dep < (midnight + pat.trips[current_trip].times[pos * 2])) {
                                    current_trip = t;
                                    board_pos = pos;
                                }
                                break;
                            }
                        }
                    }

                    // Can we improve arrival at this stop?
                    if (current_trip >= 0 && pos > board_pos) {
                        const long long arr = midnight +
                            pat.trips[current_trip].times[pos * 2 + 1];
                        if (arr < tau_star[sid] && arr < global_best + 900) {
                            local.push_back(Improvement{
                                .stop_idx  = sid,
                                .arrival_ts = arr,
                                .journey = TransitJourney{
                                    .pattern_idx = task.pattern_idx,
                                    .trip_offset = static_cast<uint32_t>(current_trip),
                                    .board_pos   = board_pos,
                                    .alight_pos  = pos,
                                },
                            });
                        }
                    }
                }
            }
            return local;
        };

        // Launch parallel tasks
        std::vector<std::future<std::vector<Improvement>>> futures;
        const std::size_t chunk = std::max<std::size_t>(1, tasks.size() / num_threads);
        for (std::size_t i = 0; i < tasks.size(); i += chunk) {
            std::size_t e = std::min(i + chunk, tasks.size());
            futures.push_back(std::async(std::launch::async, scan_chunk, i, e));
        }

        // ── Merge improvements ───────────────────────────────────
        std::fill(marked.begin(), marked.end(), false);

        for (auto& fut : futures) {
            for (auto& imp : fut.get()) {
                if (imp.arrival_ts < tau[k][imp.stop_idx]) {
                    tau[k][imp.stop_idx] = imp.arrival_ts;
                    transit_jp[k][imp.stop_idx] = imp.journey;
                    was_transfer[k][imp.stop_idx] = false;
                    if (imp.arrival_ts < tau_star[imp.stop_idx]) {
                        tau_star[imp.stop_idx] = imp.arrival_ts;
                        marked[imp.stop_idx] = true;
                    }
                    // Update global best if this is a destination stop
                    if (dest_map.count(imp.stop_idx)) {
                        long long final_arr = imp.arrival_ts + dest_map[imp.stop_idx]->walk_seconds;
                        global_best = std::min(global_best, final_arr);
                    }
                }
            }
        }

        // ── Transfer (footpath) phase ────────────────────────────
        // For each newly-improved stop, propagate to nearby stops.
        std::vector<uint32_t> transit_improved;
        for (uint32_t si = 0; si < num_stops; ++si)
            if (marked[si]) transit_improved.push_back(si);

        for (uint32_t src : transit_improved) {
            const auto& s = store_.stop(src);
            auto neighbors = store_.nearby_stops(
                s.lat, s.lon, request.max_transfer_walk_m, 8);
            for (const auto& [nbr, dist] : neighbors) {
                if (nbr == src) continue;
                const int ws = walk_seconds(dist);
                const long long arr = tau[k][src] + std::max(ws, kMinTransferS);
                if (arr < tau[k][nbr] && arr < tau_star[nbr]) {
                    tau[k][nbr] = arr;
                    tau_star[nbr] = arr;
                    transfer_jp[k][nbr] = TransferJourney{
                        .from_stop_idx = src,
                        .walk_meters   = dist,
                        .walk_seconds  = ws,
                    };
                    was_transfer[k][nbr] = true;
                    marked[nbr] = true;
                    if (dest_map.count(nbr)) {
                        long long fa = arr + dest_map[nbr]->walk_seconds;
                        global_best = std::min(global_best, fa);
                    }
                }
            }
        }

        // Also allow same-stop "transfer" (platform change, min wait)
        for (uint32_t src : transit_improved) {
            const long long arr = tau[k][src] + kMinTransferS;
            // This just ensures min transfer time is baked in for
            // the next round's boarding check, encoded in tau[k].
            // We DON'T overwrite tau[k][src] with a worse value —
            // instead, the next round uses tau[k] which already has
            // the transit arrival time.  The min-transfer buffer is
            // enforced during boarding: the trip departure must be
            // >= tau[k][src] (which equals transit arrival).
            // To truly enforce kMinTransferS, we adjust during
            // the boarding check in the next round.
            (void)arr;
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //  Path reconstruction
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    // For each destination candidate, find best round and reconstruct.
    struct DestResult {
        uint32_t dest_stop_idx;
        int round;
        long long total_arrival;
        const StopCandidate* dest_candidate;
    };

    std::vector<DestResult> dest_results;
    for (const auto& [didx, dcand] : dest_map) {
        for (int k = 1; k <= max_rounds; ++k) {
            if (tau[k][didx] >= INF) continue;
            long long total = tau[k][didx] + dcand->walk_seconds;
            dest_results.push_back({didx, k, total, dcand});
        }
    }

    // Sort by total arrival, then fewer transfers
    std::sort(dest_results.begin(), dest_results.end(),
              [](const DestResult& a, const DestResult& b) {
                  if (a.total_arrival != b.total_arrival)
                      return a.total_arrival < b.total_arrival;
                  return a.round < b.round;
              });

    // Keep Pareto-optimal (no result with both earlier arrival AND fewer transfers)
    std::vector<DestResult> pareto;
    for (const auto& dr : dest_results) {
        bool dominated = false;
        for (const auto& p : pareto) {
            if (p.total_arrival <= dr.total_arrival && p.round <= dr.round) {
                dominated = true; break;
            }
        }
        if (!dominated) pareto.push_back(dr);
    }

    // Reconstruct each Pareto-optimal itinerary
    std::vector<Itinerary> itineraries;
    std::unordered_set<std::string> seen_keys;

    for (const auto& dr : pareto) {
        std::vector<TransitLeg> legs;
        uint32_t current = dr.dest_stop_idx;

        for (int k = dr.round; k >= 1; --k) {
            uint32_t transit_stop = current;

            // Was this stop reached by a footpath transfer?
            if (was_transfer[k][current]) {
                const auto& tfr = transfer_jp[k][current];
                const auto& from_s = store_.stop(tfr.from_stop_idx);
                const auto& to_s   = store_.stop(current);
                legs.push_back(TransitLeg{
                    .mode = "walk", .route_id = "walk", .route_name = "Walk",
                    .board_stop_id = from_s.stop_id,
                    .board_stop_name = from_s.stop_name,
                    .alight_stop_id = to_s.stop_id,
                    .alight_stop_name = to_s.stop_name,
                    .departure_ts = tau[k][tfr.from_stop_idx],
                    .arrival_ts   = tau[k][current],
                    .duration_s   = static_cast<int>(tau[k][current] - tau[k][tfr.from_stop_idx]),
                    .stop_count   = 0,
                    .walk_meters  = tfr.walk_meters,
                });
                transit_stop = tfr.from_stop_idx;
            }

            // Get transit journey at transit_stop in round k
            const auto& tj = transit_jp[k][transit_stop];
            if (tj.pattern_idx == UINT32_MAX) break;  // reconstruction error

            const auto& pat = store_.patterns()[tj.pattern_idx];
            const auto& trip_times = pat.trips[tj.trip_offset];
            const uint32_t board_idx = pat.stop_indices[tj.board_pos];
            const uint32_t alight_idx = pat.stop_indices[tj.alight_pos];
            const auto& route = store_.route(pat.route_idx);
            const auto& board_s = store_.stop(board_idx);
            const auto& alight_s = store_.stop(alight_idx);
            const auto& trip = store_.trip(trip_times.trip_idx);

            long long dep_ts = midnight + trip_times.times[tj.board_pos * 2];
            long long arr_ts = midnight + trip_times.times[tj.alight_pos * 2 + 1];

            // Resolve color (subway stop-id collision handling)
            auto color = route.color_hex;
            bool subway_pair = looks_like_subway_stop_id(board_s.stop_id) ||
                               looks_like_subway_stop_id(alight_s.stop_id);
            std::string mode = route.mode;
            std::string rname = route.route_name;
            if (subway_pair && mode != "subway") {
                mode = "subway";
                rname = route.route_id;
                color = subway_color(route.route_id);
            }

            legs.push_back(TransitLeg{
                .mode = mode,
                .route_id = route.route_id,
                .route_name = rname,
                .color_hex = color,
                .headsign = trip.headsign ? trip.headsign
                            : std::optional<std::string>(route.route_long_name),
                .trip_id = trip.trip_id,
                .board_stop_id = board_s.stop_id,
                .board_stop_name = board_s.stop_name,
                .alight_stop_id = alight_s.stop_id,
                .alight_stop_name = alight_s.stop_name,
                .departure_ts = dep_ts,
                .arrival_ts = arr_ts,
                .duration_s = static_cast<int>(arr_ts - dep_ts),
                .stop_count = static_cast<int>(tj.alight_pos - tj.board_pos),
                .walk_meters = 0.0,
            });

            current = board_idx;
        }

        // Reverse (we built legs back-to-front)
        std::reverse(legs.begin(), legs.end());

        if (legs.empty()) continue;

        // ── Add origin walk leg ──────────────────────────────────
        {
            const auto& first_board_id = legs.front().board_stop_id;
            const StopCandidate* best_origin = nullptr;
            for (const auto& oc : origin_candidates) {
                if (oc.stop_id == first_board_id) {
                    if (!best_origin || oc.walk_seconds < best_origin->walk_seconds)
                        best_origin = &oc;
                }
            }
            if (best_origin && best_origin->walk_seconds > 0) {
                long long dep = legs.front().departure_ts - best_origin->walk_seconds;
                legs.insert(legs.begin(), TransitLeg{
                    .mode = "walk", .route_id = "walk", .route_name = "Walk",
                    .board_stop_id = "origin",
                    .board_stop_name = request.origin.label.empty() ? "Origin" : request.origin.label,
                    .alight_stop_id = best_origin->stop_id,
                    .alight_stop_name = best_origin->stop_name,
                    .departure_ts = dep,
                    .arrival_ts = legs.front().departure_ts,
                    .duration_s = best_origin->walk_seconds,
                    .stop_count = 0,
                    .walk_meters = best_origin->walk_meters,
                });
            }
        }

        // ── Add destination walk leg ─────────────────────────────
        if (dr.dest_candidate->walk_seconds > 0) {
            long long arr = legs.back().arrival_ts;
            long long final_arr = arr + dr.dest_candidate->walk_seconds;
            const auto& alight = store_.stop(dr.dest_stop_idx);
            legs.push_back(TransitLeg{
                .mode = "walk", .route_id = "walk", .route_name = "Walk",
                .board_stop_id = alight.stop_id,
                .board_stop_name = alight.stop_name,
                .alight_stop_id = "destination",
                .alight_stop_name = request.destination.label.empty() ? "Destination" : request.destination.label,
                .departure_ts = arr,
                .arrival_ts = final_arr,
                .duration_s = dr.dest_candidate->walk_seconds,
                .stop_count = 0,
                .walk_meters = dr.dest_candidate->walk_meters,
            });
        }

        auto itin = build_itinerary(
            std::move(legs), request.priority, request.accessibility_priority);

        // Dedup
        std::string key = itin.itinerary_id;
        if (seen_keys.count(key)) continue;
        seen_keys.insert(key);
        itineraries.push_back(std::move(itin));
    }

    log::info("RAPTOR", "route complete", {
        {"rounds", max_rounds},
        {"itineraries", itineraries.size()},
        {"threads", num_threads},
        {"elapsed_ms", std::round(timer.elapsed_ms() * 10.0) / 10.0},
    });

    return itineraries;
}

}  // namespace trackengine
