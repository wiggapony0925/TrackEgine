// ── In-Memory Schedule Store — Implementation ────────────────────
// Loads GTFS data from SQLite into flat contiguous vectors, builds
// RAPTOR route-patterns and CSA connections.

#include "trackengine/schedule_store.hpp"
#include "trackengine/logger.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <numeric>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include <sqlite3.h>

namespace trackengine {

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Duplicated utility functions (pure, no shared state)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
namespace {

constexpr double kEarthRadiusM = 6'371'009.0;

double haversine_m(double lat1, double lon1, double lat2, double lon2) {
    const double rlat1 = lat1 * M_PI / 180.0;
    const double rlat2 = lat2 * M_PI / 180.0;
    const double dlat  = (lat2 - lat1) * M_PI / 180.0;
    const double dlon  = (lon2 - lon1) * M_PI / 180.0;
    const double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
                     std::cos(rlat1) * std::cos(rlat2) *
                     std::sin(dlon / 2.0) * std::sin(dlon / 2.0);
    return kEarthRadiusM * 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

int gtfs_time_to_seconds(const std::string& gtfs_time) {
    int h = 0, m = 0, s = 0;
    char c1 = '\0', c2 = '\0';
    std::istringstream ss(gtfs_time);
    ss >> h >> c1 >> m >> c2 >> s;
    if (!ss || c1 != ':' || c2 != ':') return 0;
    return h * 3600 + m * 60 + s;
}

bool looks_like_subway_stop_id(const std::string& sid) {
    return sid.size() == 4 && (sid[3] == 'N' || sid[3] == 'S') &&
           std::all_of(sid.begin(), sid.begin() + 3,
                       [](unsigned char c) { return std::isalnum(c) != 0; });
}

std::string parent_stop_id(const std::string& sid) {
    if (looks_like_subway_stop_id(sid))
        return sid.substr(0, sid.size() - 1);
    return sid;
}

std::string infer_mode(
    const std::string& route_id,
    const std::optional<int>& route_type,
    const std::optional<std::string>& route_long_name
) {
    if (route_type && (*route_type == 3 || (*route_type >= 700 && *route_type <= 799)))
        return "bus";
    if ((route_type && *route_type == 1) || route_id == "SI")
        return "subway";
    if (route_id.size() == 1 && route_id[0] >= '1' && route_id[0] <= '7')
        return "subway";
    if (route_type && *route_type == 2) {
        static const std::unordered_set<std::string> mnr = {
            "hudson","harlem","new haven","new canaan","danbury","waterbury"};
        std::string branch = route_long_name.value_or("");
        std::transform(branch.begin(), branch.end(), branch.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return mnr.count(branch) ? "mnr" : "lirr";
    }
    if (route_id.size() == 1 || route_id == "SI" || route_id == "SIR")
        return "subway";
    return "bus";
}

std::string route_display_name(
    const std::string& route_id,
    const std::optional<std::string>& short_name,
    const std::optional<std::string>& long_name
) {
    if (route_id.size() == 1 && route_id[0] >= '1' && route_id[0] <= '7')
        return route_id;
    if (short_name && !short_name->empty()) return *short_name;
    if (long_name && !long_name->empty()) return *long_name;
    return route_id;
}

std::optional<std::string> normalize_color(const std::optional<std::string>& raw) {
    if (!raw || raw->empty()) return std::nullopt;
    std::string c = *raw;
    if (!c.empty() && c[0] == '#') c.erase(c.begin());
    if (c.size() == 3) c = {c[0],c[0],c[1],c[1],c[2],c[2]};
    if (c.size() != 6) return std::nullopt;
    std::transform(c.begin(), c.end(), c.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return "#" + c;
}

// ── Brand colors (duplicated from engine_service.cpp) ────────────

struct BrandColorMaps {
    std::unordered_map<std::string, std::string> subway;
    std::unordered_map<std::string, std::string> mode_defaults;
};

BrandColorMaps load_brand_colors_local() {
    BrandColorMaps maps;
    std::vector<std::string> candidates;
    const char* env = std::getenv("TRACK_BRAND_COLORS_JSON");
    if (env && env[0]) candidates.emplace_back(env);
    candidates.emplace_back("config/brand_colors.json");
    candidates.emplace_back("../config/brand_colors.json");
    for (const auto& p : candidates) {
        if (!std::filesystem::exists(p)) continue;
        try {
            std::ifstream f(p);
            if (!f.is_open()) continue;
            auto j = nlohmann::json::parse(f);
            if (j.contains("subway") && j["subway"].is_object())
                for (auto& [k,v] : j["subway"].items())
                    if (v.is_string()) maps.subway[k] = v.get<std::string>();
            if (j.contains("mode_defaults") && j["mode_defaults"].is_object())
                for (auto& [k,v] : j["mode_defaults"].items())
                    if (v.is_string()) maps.mode_defaults[k] = v.get<std::string>();
            return maps;
        } catch (...) {}
    }
    maps.subway = {
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
    maps.mode_defaults = {
        {"subway","#0062CF"},{"bus","#0078C6"},
        {"lirr","#0073BF"},{"mnr","#005A8C"},
    };
    return maps;
}

const BrandColorMaps& brand_colors_local() {
    static const BrandColorMaps m = load_brand_colors_local();
    return m;
}

std::optional<std::string> resolve_route_color(
    const std::string& route_id,
    const std::string& mode,
    const std::optional<std::string>& raw_color
) {
    const auto& bc = brand_colors_local();
    if (mode == "subway") {
        if (auto it = bc.subway.find(route_id); it != bc.subway.end())
            return it->second;
    }
    if (auto it = bc.mode_defaults.find(mode); it != bc.mode_defaults.end())
        return it->second;
    return normalize_color(raw_color);
}

// ── SQLite helpers ───────────────────────────────────────────────

std::string text_col(sqlite3_stmt* s, int i) {
    const auto* p = sqlite3_column_text(s, i);
    return p ? std::string(reinterpret_cast<const char*>(p)) : std::string();
}

std::optional<std::string> opt_text(sqlite3_stmt* s, int i) {
    if (sqlite3_column_type(s, i) == SQLITE_NULL) return std::nullopt;
    return text_col(s, i);
}

std::optional<int> opt_int(sqlite3_stmt* s, int i) {
    if (sqlite3_column_type(s, i) == SQLITE_NULL) return std::nullopt;
    return sqlite3_column_int(s, i);
}

}  // anonymous namespace

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Static members
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const std::vector<std::pair<uint32_t, uint32_t>> ScheduleStore::kEmptyPatterns;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Constructor — load everything from SQLite
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ScheduleStore::ScheduleStore(const std::string& db_path) {
    log::Timer timer;

    if (!std::filesystem::exists(db_path)) {
        log::warn("STORE", "Schedule DB not found", {{"path", db_path}});
        return;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        log::error("STORE", "Failed to open schedule DB", {{"path", db_path}});
        return;
    }

    auto close_db = [&]() { if (db) { sqlite3_close(db); db = nullptr; } };

    try {
        // ── Load stops ───────────────────────────────────────────
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db, "SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops", -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                uint32_t idx = static_cast<uint32_t>(stops_.size());
                stops_.push_back(MemStop{
                    .idx = idx,
                    .stop_id = text_col(stmt, 0),
                    .stop_name = text_col(stmt, 1),
                    .lat = sqlite3_column_double(stmt, 2),
                    .lon = sqlite3_column_double(stmt, 3),
                });
                stop_id_map_[stops_.back().stop_id] = idx;
            }
            if (stmt) sqlite3_finalize(stmt);
        }

        // ── Load routes ──────────────────────────────────────────
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db,
                "SELECT route_id, route_short_name, route_long_name, route_color, route_type FROM routes",
                -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                uint32_t idx = static_cast<uint32_t>(routes_.size());
                const auto rid   = text_col(stmt, 0);
                const auto sname = opt_text(stmt, 1);
                const auto lname = opt_text(stmt, 2);
                const auto rcol  = opt_text(stmt, 3);
                const auto rtype = opt_int(stmt, 4);
                const auto mode  = infer_mode(rid, rtype, lname);
                routes_.push_back(MemRoute{
                    .idx = idx,
                    .route_id = rid,
                    .route_name = route_display_name(rid, sname, lname),
                    .route_long_name = lname.value_or(rid),
                    .color_hex = resolve_route_color(rid, mode, rcol),
                    .route_type = rtype,
                    .mode = mode,
                });
                route_id_map_[rid] = idx;
            }
            if (stmt) sqlite3_finalize(stmt);
        }

        // ── Load trips ───────────────────────────────────────────
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db,
                "SELECT trip_id, route_id, service_id, trip_headsign FROM trips",
                -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                uint32_t idx = static_cast<uint32_t>(trips_.size());
                const auto rid = text_col(stmt, 1);
                uint32_t ridx = 0;
                if (auto it = route_id_map_.find(rid); it != route_id_map_.end())
                    ridx = it->second;
                trips_.push_back(MemTrip{
                    .idx = idx,
                    .trip_id = text_col(stmt, 0),
                    .route_idx = ridx,
                    .service_id = text_col(stmt, 2),
                    .headsign = opt_text(stmt, 3),
                });
                trip_id_map_[trips_.back().trip_id] = idx;
            }
            if (stmt) sqlite3_finalize(stmt);
        }

        // ── Load stop_times ──────────────────────────────────────
        struct RawST {
            uint32_t trip_idx;
            uint32_t stop_idx;
            int arr_s;
            int dep_s;
            int seq;
        };
        std::vector<RawST> raw_st;
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db,
                "SELECT trip_id, stop_id, arrival_time, departure_time, stop_sequence "
                "FROM stop_times ORDER BY trip_id, stop_sequence",
                -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                const auto tid = text_col(stmt, 0);
                const auto sid = text_col(stmt, 1);
                auto ti = trip_id_map_.find(tid);
                auto si = stop_id_map_.find(sid);
                if (ti == trip_id_map_.end() || si == stop_id_map_.end()) continue;
                stops_with_departures_.insert(si->second);
                raw_st.push_back(RawST{
                    .trip_idx = ti->second,
                    .stop_idx = si->second,
                    .arr_s = gtfs_time_to_seconds(text_col(stmt, 2)),
                    .dep_s = gtfs_time_to_seconds(text_col(stmt, 3)),
                    .seq = sqlite3_column_int(stmt, 4),
                });
            }
            if (stmt) sqlite3_finalize(stmt);
        }

        // ── Load calendar ────────────────────────────────────────
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db,
                "SELECT service_id, monday, tuesday, wednesday, thursday, "
                "friday, saturday, sunday, start_date, end_date FROM calendar",
                -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                CalEntry e;
                e.service_id = text_col(stmt, 0);
                for (int d = 0; d < 7; ++d) e.days[d] = sqlite3_column_int(stmt, 1 + d) == 1;
                const auto sd = text_col(stmt, 8);
                const auto ed = text_col(stmt, 9);
                try { e.start_date = std::stoi(sd); } catch (...) {}
                try { e.end_date = std::stoi(ed); } catch (...) {}
                calendar_.push_back(std::move(e));
            }
            if (stmt) sqlite3_finalize(stmt);
        }
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db,
                "SELECT service_id, date, exception_type FROM calendar_dates",
                -1, &stmt, nullptr);
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW) {
                CalDateEntry e;
                e.service_id = text_col(stmt, 0);
                try { e.date = std::stoi(text_col(stmt, 1)); } catch (...) {}
                e.exception_type = sqlite3_column_int(stmt, 2);
                calendar_dates_.push_back(std::move(e));
            }
            if (stmt) sqlite3_finalize(stmt);
        }

        close_db();

        // ── Build route patterns ─────────────────────────────────
        // Group stop_times by trip, then group trips with same
        // (route_idx, stop_sequence) into patterns.
        {
            // 1. Group by trip
            std::unordered_map<uint32_t, std::vector<RawST>> by_trip;
            for (auto& st : raw_st)
                by_trip[st.trip_idx].push_back(st);

            // 2. For each trip, compute a pattern key
            struct TripPattern {
                uint32_t trip_idx;
                uint32_t route_idx;
                std::vector<uint32_t> stop_seq;    // ordered stop indices
                std::vector<int> times;             // flat [dep0,arr0,dep1,arr1,…]
            };

            // pattern_key → list of TripPatterns
            std::unordered_map<std::string, std::vector<TripPattern>> pmap;

            for (auto& [trip_idx, sts] : by_trip) {
                std::sort(sts.begin(), sts.end(),
                          [](const RawST& a, const RawST& b) { return a.seq < b.seq; });
                TripPattern tp;
                tp.trip_idx = trip_idx;
                tp.route_idx = trips_[trip_idx].route_idx;
                for (auto& st : sts) {
                    tp.stop_seq.push_back(st.stop_idx);
                    tp.times.push_back(st.dep_s);
                    tp.times.push_back(st.arr_s);
                }
                // Key = route_idx + "|" + ordered stop indices
                std::string key = std::to_string(tp.route_idx) + "|";
                for (auto s : tp.stop_seq) key += std::to_string(s) + ",";
                pmap[key].push_back(std::move(tp));
            }
            raw_st.clear();  // free memory

            // 3. Build RoutePatterns
            for (auto& [key, trip_patterns] : pmap) {
                uint32_t pidx = static_cast<uint32_t>(patterns_.size());
                RoutePattern pat;
                pat.idx = pidx;
                pat.route_idx = trip_patterns.front().route_idx;
                pat.stop_indices = trip_patterns.front().stop_seq;

                // Sort trips by departure at first stop
                std::sort(trip_patterns.begin(), trip_patterns.end(),
                          [](const TripPattern& a, const TripPattern& b) {
                              return a.times.empty() ? true :
                                     b.times.empty() ? false :
                                     a.times[0] < b.times[0];
                          });

                for (auto& tp : trip_patterns) {
                    pat.trips.push_back(PatternTrip{
                        .trip_idx = tp.trip_idx,
                        .times = std::move(tp.times),
                    });
                }
                patterns_.push_back(std::move(pat));
            }
        }

        // ── Build stop → pattern index ───────────────────────────
        stop_pattern_idx_.resize(stops_.size());
        for (uint32_t pi = 0; pi < static_cast<uint32_t>(patterns_.size()); ++pi) {
            const auto& pat = patterns_[pi];
            for (uint32_t pos = 0; pos < static_cast<uint32_t>(pat.stop_indices.size()); ++pos) {
                stop_pattern_idx_[pat.stop_indices[pos]].emplace_back(pi, pos);
            }
        }

        // ── Build CSA connections ────────────────────────────────
        for (const auto& pat : patterns_) {
            for (const auto& trip : pat.trips) {
                for (uint32_t i = 0; i + 1 < static_cast<uint32_t>(pat.stop_indices.size()); ++i) {
                    connections_.push_back(MemConnection{
                        .dep_stop_idx = pat.stop_indices[i],
                        .arr_stop_idx = pat.stop_indices[i + 1],
                        .trip_idx = trip.trip_idx,
                        .route_idx = pat.route_idx,
                        .departure_s = trip.times[i * 2],
                        .arrival_s = trip.times[(i + 1) * 2 + 1],
                    });
                }
            }
        }
        std::sort(connections_.begin(), connections_.end(),
                  [](const MemConnection& a, const MemConnection& b) {
                      return a.departure_s < b.departure_s;
                  });

        // ── Build spatial index (sort by latitude) ───────────────
        stops_by_lat_.resize(stops_.size());
        std::iota(stops_by_lat_.begin(), stops_by_lat_.end(), 0u);
        std::sort(stops_by_lat_.begin(), stops_by_lat_.end(),
                  [this](uint32_t a, uint32_t b) { return stops_[a].lat < stops_[b].lat; });

        loaded_ = true;

        log::info("STORE", "ScheduleStore loaded", {
            {"stops", stops_.size()},
            {"routes", routes_.size()},
            {"trips", trips_.size()},
            {"patterns", patterns_.size()},
            {"connections", connections_.size()},
            {"load_ms", std::round(timer.elapsed_ms() * 10.0) / 10.0},
        });
    } catch (const std::exception& e) {
        close_db();
        log::error("STORE", "Failed to build ScheduleStore", {{"error", e.what()}});
        loaded_ = false;
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Lookups
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

std::optional<uint32_t> ScheduleStore::stop_idx(const std::string& id) const {
    auto it = stop_id_map_.find(id);
    return it != stop_id_map_.end() ? std::optional(it->second) : std::nullopt;
}

std::optional<uint32_t> ScheduleStore::route_idx(const std::string& id) const {
    auto it = route_id_map_.find(id);
    return it != route_id_map_.end() ? std::optional(it->second) : std::nullopt;
}

const std::vector<std::pair<uint32_t, uint32_t>>&
ScheduleStore::patterns_at_stop(uint32_t stop_idx) const {
    if (stop_idx >= stop_pattern_idx_.size()) return kEmptyPatterns;
    return stop_pattern_idx_[stop_idx];
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Spatial queries
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

std::vector<std::pair<uint32_t, double>>
ScheduleStore::nearby_stops(double lat, double lon, int radius_m, std::size_t limit) const {
    const double lat_delta = radius_m / 111'000.0;
    const double min_lat = lat - lat_delta;
    const double max_lat = lat + lat_delta;

    // Binary search for latitude range in the sorted index.
    auto lower = std::lower_bound(
        stops_by_lat_.begin(), stops_by_lat_.end(), min_lat,
        [this](uint32_t idx, double v) { return stops_[idx].lat < v; });
    auto upper = std::upper_bound(
        stops_by_lat_.begin(), stops_by_lat_.end(), max_lat,
        [this](double v, uint32_t idx) { return v < stops_[idx].lat; });

    const double lon_div = 111'320.0 * std::cos(lat * M_PI / 180.0);
    const double lon_delta = (lon_div == 0.0) ? lat_delta : radius_m / lon_div;
    const double min_lon = lon - lon_delta;
    const double max_lon = lon + lon_delta;

    std::vector<std::pair<uint32_t, double>> results;
    for (auto it = lower; it != upper; ++it) {
        const uint32_t idx = *it;
        const auto& s = stops_[idx];
        if (s.lon < min_lon || s.lon > max_lon) continue;
        if (!stops_with_departures_.count(idx)) continue;
        const double d = haversine_m(lat, lon, s.lat, s.lon);
        if (d <= static_cast<double>(radius_m))
            results.emplace_back(idx, d);
    }
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    // Deduplicate by parent station (keep ≤4 per station)
    std::unordered_map<std::string, int> station_count;
    std::vector<std::pair<uint32_t, double>> deduped;
    for (const auto& [idx, dist] : results) {
        auto& cnt = station_count[parent_stop_id(stops_[idx].stop_id)];
        if (cnt < 4) {
            deduped.emplace_back(idx, dist);
            ++cnt;
        }
    }
    if (deduped.size() > limit) deduped.resize(limit);
    return deduped;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  Calendar
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

std::vector<std::string>
ScheduleStore::active_services(int yyyymmdd, int weekday) const {
    if (weekday < 0 || weekday > 6) return {};
    std::set<std::string> active;
    for (const auto& c : calendar_) {
        if (c.days[weekday] && c.start_date <= yyyymmdd && c.end_date >= yyyymmdd)
            active.insert(c.service_id);
    }
    for (const auto& d : calendar_dates_) {
        if (d.date == yyyymmdd) {
            if (d.exception_type == 1) active.insert(d.service_id);
            else if (d.exception_type == 2) active.erase(d.service_id);
        }
    }
    return {active.begin(), active.end()};
}

}  // namespace trackengine
