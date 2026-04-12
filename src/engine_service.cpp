#include "trackengine/engine_service.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <sqlite3.h>

namespace trackengine {
namespace {

constexpr double kEarthRadiusM = 6'371'009.0;
constexpr double kWalkSpeedMps = 1.45;  // NYC walkers average ~3.2 mph
constexpr int kMinTransferSeconds = 120;
constexpr int kOriginDepartureLimit = 96;
constexpr int kSecondLegDepartureLimit = 10;
constexpr int kTransferStopLimit = 10;
constexpr int kMaxIntermediateStops = 24;
constexpr std::size_t kMaxGeneratedItineraries = 96;
constexpr long long kPruningToleranceS = 900;  // 15 min tolerance for mode diversity

constexpr std::array<const char*, 7> kWeekdayColumns = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
};

constexpr std::array<const char*, 4> kImportantIndexes = {
    "idx_stop_times_trip_seq",
    "idx_stop_times_trip_stop_seq",
    "idx_stops_name",
    "idx_stop_times_stop_dept",
};

struct SqliteConnection {
    sqlite3* db = nullptr;

    explicit SqliteConnection(const std::string& db_path) {
        // Use READWRITE instead of READONLY so that SQLite can create the
        // shared-memory mapping required by WAL-mode databases.  The engine
        // never writes data — it only reads the schedule.
        const int rc = sqlite3_open_v2(
            db_path.c_str(),
            &db,
            SQLITE_OPEN_READWRITE,
            nullptr
        );
        if (rc != SQLITE_OK || db == nullptr) {
            std::string message = db != nullptr ? sqlite3_errmsg(db) : "sqlite open failed";
            if (db != nullptr) {
                sqlite3_close(db);
                db = nullptr;
            }
            throw std::runtime_error("sqlite open failed for " + db_path + ": " + message);
        }
        sqlite3_busy_timeout(db, 30'000);
    }

    ~SqliteConnection() {
        if (db != nullptr) {
            sqlite3_close(db);
        }
    }

    SqliteConnection(const SqliteConnection&) = delete;
    SqliteConnection& operator=(const SqliteConnection&) = delete;
};

class Statement final {
public:
    Statement(sqlite3* db, const std::string& sql) {
        const int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr);
        if (rc != SQLITE_OK || stmt_ == nullptr) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
    }

    ~Statement() {
        if (stmt_ != nullptr) {
            sqlite3_finalize(stmt_);
        }
    }

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    sqlite3_stmt* get() { return stmt_; }
    const sqlite3_stmt* get() const { return stmt_; }

private:
    sqlite3_stmt* stmt_ = nullptr;
};

// ── Prepared statement cache ──────────────────────────────────────
// Compiles each SQL string once per PlannerContext (i.e. per request).
// Subsequent calls with the same SQL reuse the compiled statement via
// sqlite3_reset() + sqlite3_clear_bindings(), avoiding the cost of
// sqlite3_prepare_v2 on every invocation.
class StatementCache final {
public:
    explicit StatementCache(sqlite3* db) : db_(db) {}

    ~StatementCache() {
        for (auto& [sql, stmt] : cache_) {
            if (stmt != nullptr) {
                sqlite3_finalize(stmt);
            }
        }
    }

    StatementCache(const StatementCache&) = delete;
    StatementCache& operator=(const StatementCache&) = delete;

    // Returns a ready-to-bind statement.  Caller must NOT finalize it.
    sqlite3_stmt* prepare(const std::string& sql) {
        auto it = cache_.find(sql);
        if (it != cache_.end()) {
            sqlite3_reset(it->second);
            sqlite3_clear_bindings(it->second);
            return it->second;
        }
        sqlite3_stmt* stmt = nullptr;
        const int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK || stmt == nullptr) {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
        cache_.emplace(sql, stmt);
        return stmt;
    }

private:
    sqlite3* db_;
    std::unordered_map<std::string, sqlite3_stmt*> cache_;
};

struct PlannerContext {
    sqlite3* db = nullptr;
    std::unique_ptr<StatementCache> stmts;  // compiled-statement cache
    std::unordered_map<std::string, std::optional<StopRecord>> stop_cache;
    std::unordered_map<std::string, RouteMeta> route_cache;
    std::unordered_map<std::string, std::vector<StopTimeRow>> downstream_cache;
    std::unordered_map<std::string, std::vector<std::pair<StopRecord, double>>> transfer_stop_cache;
};

struct DestinationMatch {
    StopTimeRow row;
    StopCandidate candidate;
    long long arrival_ts = 0;
};

double haversine_m(double lat1, double lon1, double lat2, double lon2) {
    const double rlat1 = lat1 * M_PI / 180.0;
    const double rlat2 = lat2 * M_PI / 180.0;
    const double dlat = (lat2 - lat1) * M_PI / 180.0;
    const double dlon = (lon2 - lon1) * M_PI / 180.0;
    const double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
                     std::cos(rlat1) * std::cos(rlat2) *
                         std::sin(dlon / 2.0) * std::sin(dlon / 2.0);
    return kEarthRadiusM * 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

std::pair<double, double> bounding_box_degrees(double radius_m, double center_lat) {
    const double lat_delta = radius_m / 111'000.0;
    const double lon_divisor = 111'320.0 * std::cos(center_lat * M_PI / 180.0);
    const double lon_delta = lon_divisor == 0.0 ? lat_delta : radius_m / lon_divisor;
    return {lat_delta, lon_delta};
}

int walk_seconds(double walk_meters) {
    return static_cast<int>(std::ceil(walk_meters / kWalkSpeedMps));
}

int gtfs_time_to_seconds(const std::string& gtfs_time) {
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    char colon1 = '\0';
    char colon2 = '\0';
    std::istringstream stream(gtfs_time);
    stream >> hours >> colon1 >> minutes >> colon2 >> seconds;
    if (!stream || colon1 != ':' || colon2 != ':') {
        return 0;
    }
    return hours * 3600 + minutes * 60 + seconds;
}

std::string seconds_to_gtfs_time(long long total_seconds) {
    if (total_seconds < 0) {
        total_seconds = 0;
    }
    const long long hours = total_seconds / 3600;
    const long long minutes = (total_seconds % 3600) / 60;
    const long long seconds = total_seconds % 60;
    std::ostringstream stream;
    stream.fill('0');
    stream.width(2);
    stream << hours;
    stream << ":";
    stream.width(2);
    stream << minutes;
    stream << ":";
    stream.width(2);
    stream << seconds;
    return stream.str();
}

long long gtfs_time_to_timestamp(long long midnight_ts, const std::string& gtfs_time) {
    // GTFS times can exceed 24:00:00 (e.g. 25:15:00 for a post-midnight
    // trip started before midnight).  The arithmetic is correct because
    // gtfs_time_to_seconds returns total seconds from service-day midnight,
    // and service_day_midnight_ts is already calibrated to the correct
    // calendar date.  No additional day-rollover adjustment is needed.
    return midnight_ts + static_cast<long long>(gtfs_time_to_seconds(gtfs_time));
}

std::string yyyymmdd_string(int value) {
    std::ostringstream stream;
    stream.fill('0');
    stream.width(8);
    stream << value;
    return stream.str();
}

std::optional<std::string> optional_string(sqlite3_stmt* stmt, int index) {
    if (sqlite3_column_type(stmt, index) == SQLITE_NULL) {
        return std::nullopt;
    }
    const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
    return text != nullptr ? std::optional<std::string>(text) : std::nullopt;
}

std::string text_or_empty(sqlite3_stmt* stmt, int index) {
    if (sqlite3_column_type(stmt, index) == SQLITE_NULL) {
        return "";
    }
    const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
    return text != nullptr ? std::string(text) : std::string();
}

std::optional<double> optional_double(const nlohmann::json& payload, const char* key) {
    if (!payload.contains(key) || payload.at(key).is_null()) {
        return std::nullopt;
    }
    return payload.at(key).get<double>();
}

std::optional<long long> optional_int64(const nlohmann::json& payload, const char* key) {
    if (!payload.contains(key) || payload.at(key).is_null()) {
        return std::nullopt;
    }
    return payload.at(key).get<long long>();
}

std::optional<std::string> optional_json_string(const nlohmann::json& payload, const char* key) {
    if (!payload.contains(key) || payload.at(key).is_null()) {
        return std::nullopt;
    }
    return payload.at(key).get<std::string>();
}

std::string route_display_name(
    const std::string& route_id,
    const std::optional<std::string>& route_short_name,
    const std::optional<std::string>& route_long_name
) {
    if (route_short_name && !route_short_name->empty()) {
        return *route_short_name;
    }
    if (route_long_name && !route_long_name->empty()) {
        return *route_long_name;
    }
    return route_id;
}

std::optional<std::string> normalize_color(const std::optional<std::string>& route_color) {
    if (!route_color || route_color->empty()) {
        return std::nullopt;
    }
    std::string color = *route_color;
    if (!color.empty() && color[0] == '#') {
        color.erase(color.begin());
    }
    if (color.size() == 3) {
        color = {
            color[0], color[0],
            color[1], color[1],
            color[2], color[2],
        };
    }
    if (color.size() != 6) {
        return std::nullopt;
    }
    std::transform(color.begin(), color.end(), color.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return "#" + color;
}

bool looks_like_subway_stop_id(const std::string& stop_id) {
    return stop_id.size() == 4 && (stop_id[3] == 'N' || stop_id[3] == 'S') &&
           std::all_of(stop_id.begin(), stop_id.begin() + 3, [](unsigned char ch) {
               return std::isalnum(ch) != 0;
           });
}

// ── Parent-station helper ─────────────────────────────────────────
// MTA GTFS uses directional platform IDs like "101N" / "101S".
// This strips the trailing direction letter to get the station ID,
// so both platforms map to "101" for grouping purposes.
std::string parent_stop_id(const std::string& stop_id) {
    if (looks_like_subway_stop_id(stop_id)) {
        return stop_id.substr(0, stop_id.size() - 1);
    }
    return stop_id;
}

std::optional<std::string> subway_color_for_route(const std::string& route_id) {
    static const std::unordered_map<std::string, std::string> kSubwayColors = {
        {"1", "#EE352E"},
        {"2", "#EE352E"},
        {"3", "#EE352E"},
        {"4", "#00933C"},
        {"5", "#00933C"},
        {"6", "#00933C"},
        {"7", "#B933AD"},
        {"A", "#0039A6"},
        {"B", "#FF6319"},
        {"C", "#0039A6"},
        {"D", "#FF6319"},
        {"E", "#0039A6"},
        {"F", "#FF6319"},
        {"G", "#6CBE45"},
        {"J", "#996633"},
        {"L", "#A7A9AC"},
        {"M", "#FF6319"},
        {"N", "#FCCC0A"},
        {"Q", "#FCCC0A"},
        {"R", "#FCCC0A"},
        {"S", "#808183"},
        {"SI", "#0039A6"},
        {"W", "#FCCC0A"},
        {"Z", "#996633"},
    };
    if (const auto found = kSubwayColors.find(route_id); found != kSubwayColors.end()) {
        return found->second;
    }
    return std::nullopt;
}

std::string infer_mode(
    const std::string& route_id,
    const std::optional<int>& route_type,
    const std::optional<std::string>& route_long_name
) {
    // GTFS route_type 3 = local bus, 700-799 = various bus categories
    if (route_type && (*route_type == 3 || (*route_type >= 700 && *route_type <= 799))) {
        return "bus";
    }
    if ((route_type && *route_type == 1) || route_id == "SI") {
        return "subway";
    }
    if (route_type && *route_type == 2) {
        static const std::unordered_set<std::string> metro_north_branches = {
            "hudson",
            "harlem",
            "new haven",
            "new canaan",
            "danbury",
            "waterbury",
        };
        std::string branch = route_long_name.value_or("");
        std::transform(branch.begin(), branch.end(), branch.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        if (metro_north_branches.contains(branch)) {
            return "mnr";
        }
        return "lirr";
    }
    // Unknown route_type: classify by route_id heuristics
    // NYC subway route_ids are single letters or single digits.
    if (route_id.size() == 1 || route_id == "SI" || route_id == "SIR") {
        return "subway";
    }
    // Default to bus — most unknown routes are bus-like.
    return "bus";
}

std::vector<std::string> existing_index_names(sqlite3* db) {
    Statement stmt(
        db,
        "SELECT name FROM sqlite_master "
        "WHERE type = 'index' AND name IS NOT NULL "
        "ORDER BY name ASC"
    );
    std::vector<std::string> indexes;
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        indexes.push_back(text_or_empty(stmt.get(), 0));
    }
    return indexes;
}

std::optional<StopRecord> fetch_stop(PlannerContext& ctx, const std::string& stop_id) {
    const auto cached = ctx.stop_cache.find(stop_id);
    if (cached != ctx.stop_cache.end()) {
        return cached->second;
    }
    static const std::string kSql =
        "SELECT stop_id, stop_name, stop_lat, stop_lon "
        "FROM stops WHERE stop_id = ?";
    auto* stmt = ctx.stmts->prepare(kSql);
    sqlite3_bind_text(stmt, 1, stop_id.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        ctx.stop_cache.emplace(stop_id, std::nullopt);
        return std::nullopt;
    }
    StopRecord stop{
        .stop_id = text_or_empty(stmt, 0),
        .stop_name = text_or_empty(stmt, 1),
        .lat = sqlite3_column_double(stmt, 2),
        .lon = sqlite3_column_double(stmt, 3),
    };
    ctx.stop_cache.emplace(stop_id, stop);
    return stop;
}

RouteMeta fetch_route(PlannerContext& ctx, const std::string& route_id) {
    const auto cached = ctx.route_cache.find(route_id);
    if (cached != ctx.route_cache.end()) {
        return cached->second;
    }
    static const std::string kSql =
        "SELECT route_id, route_short_name, route_long_name, route_color, route_type "
        "FROM routes WHERE route_id = ?";
    auto* stmt = ctx.stmts->prepare(kSql);
    sqlite3_bind_text(stmt, 1, route_id.c_str(), -1, SQLITE_TRANSIENT);
    RouteMeta meta;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto route_short_name = optional_string(stmt, 1);
        const auto route_long_name = optional_string(stmt, 2);
        const auto route_color = optional_string(stmt, 3);
        const auto route_type = sqlite3_column_type(stmt, 4) == SQLITE_NULL
                                    ? std::optional<int>()
                                    : std::optional<int>(sqlite3_column_int(stmt, 4));
        meta = RouteMeta{
            .route_id = text_or_empty(stmt, 0),
            .route_name = route_display_name(text_or_empty(stmt, 0), route_short_name, route_long_name),
            .route_long_name = route_long_name.value_or(text_or_empty(stmt, 0)),
            .color_hex = normalize_color(route_color),
            .route_type = route_type,
            .mode = infer_mode(text_or_empty(stmt, 0), route_type, route_long_name),
        };
    } else {
        // Route not in database — infer from route_id pattern.
        std::string inferred_mode = "bus";
        if (route_id.size() == 1 || route_id == "SI" || route_id == "SIR") {
            inferred_mode = "subway";
        }
        meta = RouteMeta{
            .route_id = route_id,
            .route_name = route_id,
            .route_long_name = route_id,
            .color_hex = std::nullopt,
            .route_type = std::nullopt,
            .mode = inferred_mode,
        };
    }
    ctx.route_cache.emplace(route_id, meta);
    return meta;
}

std::vector<std::string> active_service_ids(PlannerContext& ctx, int service_day_yyyymmdd, int service_weekday) {
    if (service_weekday < 0 || service_weekday >= static_cast<int>(kWeekdayColumns.size())) {
        return {};
    }
    const std::string day_value = yyyymmdd_string(service_day_yyyymmdd);
    const std::string sql =
        "SELECT service_id FROM calendar WHERE " +
        std::string(kWeekdayColumns[static_cast<std::size_t>(service_weekday)]) +
        " = 1 AND start_date <= ? AND end_date >= ?";
    Statement calendar_stmt(ctx.db, sql);
    sqlite3_bind_text(calendar_stmt.get(), 1, day_value.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(calendar_stmt.get(), 2, day_value.c_str(), -1, SQLITE_TRANSIENT);

    std::set<std::string> active;
    while (sqlite3_step(calendar_stmt.get()) == SQLITE_ROW) {
        active.insert(text_or_empty(calendar_stmt.get(), 0));
    }

    Statement dates_stmt(
        ctx.db,
        "SELECT service_id, exception_type FROM calendar_dates WHERE date = ?"
    );
    sqlite3_bind_text(dates_stmt.get(), 1, day_value.c_str(), -1, SQLITE_TRANSIENT);
    while (sqlite3_step(dates_stmt.get()) == SQLITE_ROW) {
        const std::string service_id = text_or_empty(dates_stmt.get(), 0);
        const int exception_type = sqlite3_column_int(dates_stmt.get(), 1);
        if (exception_type == 1) {
            active.insert(service_id);
        } else if (exception_type == 2) {
            active.erase(service_id);
        }
    }
    return {active.begin(), active.end()};
}

std::string placeholders(std::size_t count) {
    std::string result;
    for (std::size_t index = 0; index < count; ++index) {
        if (index > 0) {
            result += ",";
        }
        result += "?";
    }
    return result;
}

std::vector<std::pair<StopRecord, double>> nearby_stops(
    PlannerContext& ctx,
    double lat,
    double lon,
    int radius_m,
    std::size_t limit
) {
    const auto [lat_delta, lon_delta] = bounding_box_degrees(radius_m, lat);
    static const std::string kSql =
        "SELECT stop_id, stop_name, stop_lat, stop_lon "
        "FROM stops "
        "WHERE stop_lat BETWEEN ? AND ? "
        "AND stop_lon BETWEEN ? AND ?";
    auto* stmt = ctx.stmts->prepare(kSql);
    sqlite3_bind_double(stmt, 1, lat - lat_delta);
    sqlite3_bind_double(stmt, 2, lat + lat_delta);
    sqlite3_bind_double(stmt, 3, lon - lon_delta);
    sqlite3_bind_double(stmt, 4, lon + lon_delta);

    static const std::string kDepCheckSql =
        "SELECT 1 FROM stop_times WHERE stop_id = ? LIMIT 1";

    std::vector<std::pair<StopRecord, double>> results;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        StopRecord record{
            .stop_id = text_or_empty(stmt, 0),
            .stop_name = text_or_empty(stmt, 1),
            .lat = sqlite3_column_double(stmt, 2),
            .lon = sqlite3_column_double(stmt, 3),
        };
        const double distance = haversine_m(lat, lon, record.lat, record.lon);
        if (distance <= static_cast<double>(radius_m)) {
            // Skip parent stations that have no stop_times (e.g. GTFS
            // location_type=1 entries stored without that column)
            auto* dep_check = ctx.stmts->prepare(kDepCheckSql);
            sqlite3_bind_text(dep_check, 1, record.stop_id.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(dep_check) == SQLITE_ROW) {
                results.emplace_back(record, distance);
            }
        }
    }
    std::sort(results.begin(), results.end(), [](const auto& left, const auto& right) {
        return std::tie(left.second, left.first.stop_name) < std::tie(right.second, right.first.stop_name);
    });

    // Group by parent station so directional platforms (101N/101S)
    // don't consume multiple limit slots for the same physical station.
    std::unordered_map<std::string, std::size_t> station_slots;
    std::vector<std::pair<StopRecord, double>> deduped;
    for (const auto& entry : results) {
        const std::string parent = parent_stop_id(entry.first.stop_id);
        auto& count = station_slots[parent];
        if (count < 2) {  // keep at most 2 platforms per station (N+S)
            deduped.push_back(entry);
            ++count;
        }
    }
    if (deduped.size() > limit) {
        deduped.resize(limit);
    }
    return deduped;
}

// Mode-aware nearby stops: finds stops within a bounding box that serve
// at least one route of the requested transit mode.  Uses the precomputed
// stop_modes table for O(1) lookups instead of expensive EXISTS subqueries.
// Falls back silently to an empty result when stop_modes does not exist
// (e.g. during tests or before prepare_schedule_db.sh runs).
std::vector<std::pair<StopRecord, double>> nearby_stops_for_mode(
    PlannerContext& ctx,
    double lat,
    double lon,
    int radius_m,
    const std::string& mode,
    std::size_t limit
) {
    const auto [lat_delta, lon_delta] = bounding_box_degrees(radius_m, lat);

    // Map mode → route_type SQL condition
    std::string stop_mode_condition;
    std::string route_filter_condition;
    if (mode == "bus") {
        stop_mode_condition =
            "(sm.route_type = 3 OR (sm.route_type >= 700 AND sm.route_type <= 799))";
        route_filter_condition =
            "(r.route_type = 3 OR (r.route_type >= 700 AND r.route_type <= 799))";
    } else if (mode == "subway") {
        stop_mode_condition = "(sm.route_type = 1)";
        route_filter_condition = "(r.route_type = 1 OR r.route_id IN ('SI', 'SIR'))";
    } else {
        // lirr, mnr — both GTFS route_type 2
        stop_mode_condition = "(sm.route_type = 2)";
        route_filter_condition = "(r.route_type = 2)";
    }

    const std::string sql =
        "SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon "
        "FROM stops s "
        "JOIN stop_modes sm ON sm.stop_id = s.stop_id "
        "WHERE s.stop_lat BETWEEN ? AND ? "
        "AND s.stop_lon BETWEEN ? AND ? "
        "AND " + stop_mode_condition;

    // The stop_modes table is created by prepare_schedule_db.sh.
    // If it doesn't exist yet, fall back to a direct join so routing still
    // works against partially prepared databases.
    sqlite3_stmt* raw_stmt = nullptr;
    const int rc = sqlite3_prepare_v2(ctx.db, sql.c_str(), -1, &raw_stmt, nullptr);
    const bool has_stop_modes = rc == SQLITE_OK && raw_stmt != nullptr;
    if (raw_stmt != nullptr) {
        sqlite3_finalize(raw_stmt);
    }

    sqlite3_stmt* stmt = nullptr;
    if (has_stop_modes) {
        stmt = ctx.stmts->prepare(sql);
    } else {
        const std::string fallback_sql =
            "SELECT DISTINCT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon "
            "FROM stops s "
            "JOIN stop_times st ON st.stop_id = s.stop_id "
            "JOIN trips t ON t.trip_id = st.trip_id "
            "JOIN routes r ON r.route_id = t.route_id "
            "WHERE s.stop_lat BETWEEN ? AND ? "
            "AND s.stop_lon BETWEEN ? AND ? "
            "AND " + route_filter_condition;
        stmt = ctx.stmts->prepare(fallback_sql);
    }
    sqlite3_bind_double(stmt, 1, lat - lat_delta);
    sqlite3_bind_double(stmt, 2, lat + lat_delta);
    sqlite3_bind_double(stmt, 3, lon - lon_delta);
    sqlite3_bind_double(stmt, 4, lon + lon_delta);

    std::vector<std::pair<StopRecord, double>> results;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        StopRecord record{
            .stop_id = text_or_empty(stmt, 0),
            .stop_name = text_or_empty(stmt, 1),
            .lat = sqlite3_column_double(stmt, 2),
            .lon = sqlite3_column_double(stmt, 3),
        };
        const double distance = haversine_m(lat, lon, record.lat, record.lon);
        if (distance <= static_cast<double>(radius_m)) {
            results.emplace_back(record, distance);
        }
    }
    std::sort(results.begin(), results.end(), [](const auto& left, const auto& right) {
        return std::tie(left.second, left.first.stop_name) < std::tie(right.second, right.first.stop_name);
    });
    if (results.size() > limit) {
        results.resize(limit);
    }
    return results;
}

double distance_to_stop(const LocationInput& location, const StopRecord& stop) {
    if (!location.lat || !location.lon) {
        return 0.0;
    }
    return haversine_m(*location.lat, *location.lon, stop.lat, stop.lon);
}

std::vector<StopCandidate> spatially_diverse_candidates(
    const std::vector<StopCandidate>& candidates,
    std::size_t limit,
    double min_spacing_m
) {
    if (candidates.size() <= limit) {
        return candidates;
    }

    std::vector<StopCandidate> selected;
    std::vector<StopCandidate> deferred;
    selected.reserve(limit);
    deferred.reserve(candidates.size());

    for (const auto& candidate : candidates) {
        const bool too_close = std::any_of(
            selected.begin(),
            selected.end(),
            [&](const StopCandidate& kept) {
                return haversine_m(
                           candidate.lat,
                           candidate.lon,
                           kept.lat,
                           kept.lon
                       ) < min_spacing_m;
            }
        );
        if (too_close) {
            deferred.push_back(candidate);
            continue;
        }
        selected.push_back(candidate);
        if (selected.size() >= limit) {
            return selected;
        }
    }

    for (const auto& candidate : deferred) {
        if (selected.size() >= limit) {
            break;
        }
        selected.push_back(candidate);
    }
    return selected;
}

// ── Parent-station expansion ──────────────────────────────────────
// When the caller supplies a parent stop_id (e.g. "725") that has no
// stop_times rows, expand it to its directional children ("725N",
// "725S") so the planner can find departures.  Also handles
// alphanumeric prefixes like "R16" → "R16N"/"R16S" and "D24" → etc.
bool stop_has_departures(PlannerContext& ctx, const std::string& stop_id) {
    static const std::string kSql = "SELECT 1 FROM stop_times WHERE stop_id = ? LIMIT 1";
    auto* stmt = ctx.stmts->prepare(kSql);
    sqlite3_bind_text(stmt, 1, stop_id.c_str(), -1, SQLITE_TRANSIENT);
    return sqlite3_step(stmt) == SQLITE_ROW;
}

std::vector<StopRecord> expand_parent_stop(PlannerContext& ctx, const std::string& stop_id) {
    std::vector<StopRecord> children;
    for (const char suffix : {'N', 'S'}) {
        const std::string child_id = stop_id + suffix;
        if (const auto child = fetch_stop(ctx, child_id)) {
            children.push_back(*child);
        }
    }
    return children;
}

std::vector<StopCandidate> resolve_candidates(
    PlannerContext& ctx,
    const LocationInput& location,
    int max_walk_m,
    const std::unordered_set<std::string>& modes
) {
    std::vector<StopCandidate> candidates;
    std::unordered_set<std::string> seen;
    if (location.stop_id) {
        if (const auto stop = fetch_stop(ctx, *location.stop_id)) {
            // If the stop_id is a parent station with no stop_times,
            // expand to directional children (e.g. "725" → "725N", "725S").
            if (!stop_has_departures(ctx, stop->stop_id)) {
                const auto children = expand_parent_stop(ctx, stop->stop_id);
                for (const auto& child : children) {
                    if (seen.contains(child.stop_id)) continue;
                    const double walk_meters = distance_to_stop(location, child);
                    candidates.push_back(StopCandidate{
                        .stop_id = child.stop_id,
                        .stop_name = child.stop_name,
                        .lat = child.lat,
                        .lon = child.lon,
                        .walk_meters = walk_meters,
                        .walk_seconds = walk_seconds(walk_meters),
                    });
                    seen.insert(child.stop_id);
                }
            } else {
                const double walk_meters = distance_to_stop(location, *stop);
                candidates.push_back(StopCandidate{
                    .stop_id = stop->stop_id,
                    .stop_name = stop->stop_name,
                    .lat = stop->lat,
                    .lon = stop->lon,
                    .walk_meters = walk_meters,
                    .walk_seconds = walk_seconds(walk_meters),
                });
                seen.insert(stop->stop_id);
            }
        }
    }
    if (!location.lat || !location.lon) {
        return candidates;
    }

    auto nearby = nearby_stops(ctx, *location.lat, *location.lon, max_walk_m, 10);
    if (nearby.empty() && candidates.empty()) {
        nearby = nearby_stops(
            ctx,
            *location.lat,
            *location.lon,
            std::max(max_walk_m * 2, 2500),
            5
        );
    }
    for (const auto& [stop, distance] : nearby) {
        if (seen.contains(stop.stop_id)) {
            continue;
        }
        seen.insert(stop.stop_id);
        candidates.push_back(StopCandidate{
            .stop_id = stop.stop_id,
            .stop_name = stop.stop_name,
            .lat = stop.lat,
            .lon = stop.lon,
            .walk_meters = distance,
            .walk_seconds = walk_seconds(distance),
        });
    }

    // ── Mode-balanced augmentation ──
    // In dense subway areas, the 10 closest stops may all be subway.
    // For each requested mode, ensure at least a few stops are present
    // by running a mode-specific spatial query (EXISTS subquery, fast).
    constexpr std::size_t kPerModeMin = 4;
    std::unordered_set<std::string> mode_augmented_ids;
    for (const auto& mode : modes) {
        auto mode_nearby = nearby_stops_for_mode(
            ctx, *location.lat, *location.lon, max_walk_m, mode, kPerModeMin
        );
        for (const auto& [stop, distance] : mode_nearby) {
            if (seen.contains(stop.stop_id)) {
                continue;
            }
            seen.insert(stop.stop_id);
            mode_augmented_ids.insert(stop.stop_id);
            candidates.push_back(StopCandidate{
                .stop_id = stop.stop_id,
                .stop_name = stop.stop_name,
                .lat = stop.lat,
                .lon = stop.lon,
                .walk_meters = distance,
                .walk_seconds = walk_seconds(distance),
            });
        }
    }

    std::sort(candidates.begin(), candidates.end(), [](const auto& left, const auto& right) {
        return std::tie(left.walk_seconds, left.stop_name) < std::tie(right.walk_seconds, right.stop_name);
    });
    constexpr std::size_t kCandidateLimit = 12;
    constexpr double kCandidateSpacingM = 180.0;
    if (candidates.size() > kCandidateLimit) {
        // Split: mode-augmented stops are exempt from spatial diversity
        // because a bus stop near a subway station is NOT a substitute.
        std::vector<StopCandidate> general;
        std::vector<StopCandidate> mode_essential;
        for (auto& c : candidates) {
            if (mode_augmented_ids.contains(c.stop_id)) {
                mode_essential.push_back(std::move(c));
            } else {
                general.push_back(std::move(c));
            }
        }
        const std::size_t general_limit =
            kCandidateLimit > mode_essential.size()
                ? kCandidateLimit - mode_essential.size()
                : 0;
        if (general.size() > general_limit) {
            general = spatially_diverse_candidates(
                general,
                general_limit,
                kCandidateSpacingM
            );
        }
        candidates.clear();
        candidates.reserve(general.size() + mode_essential.size());
        for (auto& g : general) candidates.push_back(std::move(g));
        for (auto& m : mode_essential) candidates.push_back(std::move(m));
        std::sort(candidates.begin(), candidates.end(), [](const auto& left, const auto& right) {
            return std::tie(left.walk_seconds, left.stop_name) < std::tie(right.walk_seconds, right.stop_name);
        });
    }
    return candidates;
}

std::vector<DepartureRow> find_departures(
    PlannerContext& ctx,
    const std::vector<std::string>& stop_ids,
    const std::vector<std::string>& service_ids,
    const std::string& depart_from,
    const std::string& depart_to,
    int limit
) {
    if (stop_ids.empty() || service_ids.empty()) {
        return {};
    }
    const std::string sql =
        "SELECT st.stop_id, st.departure_time, st.stop_sequence, "
        "t.trip_id, t.route_id, t.trip_headsign "
        "FROM stop_times st "
        "JOIN trips t ON st.trip_id = t.trip_id "
        "WHERE st.stop_id IN (" + placeholders(stop_ids.size()) + ") "
        "AND t.service_id IN (" + placeholders(service_ids.size()) + ") "
        "AND st.departure_time >= ? "
        "AND st.departure_time <= ? "
        "ORDER BY st.departure_time ASC, st.stop_sequence ASC "
        "LIMIT ?";

    auto* stmt = ctx.stmts->prepare(sql);
    int bind_index = 1;
    for (const auto& stop_id : stop_ids) {
        sqlite3_bind_text(stmt, bind_index++, stop_id.c_str(), -1, SQLITE_TRANSIENT);
    }
    for (const auto& service_id : service_ids) {
        sqlite3_bind_text(stmt, bind_index++, service_id.c_str(), -1, SQLITE_TRANSIENT);
    }
    sqlite3_bind_text(stmt, bind_index++, depart_from.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, bind_index++, depart_to.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, bind_index, limit);

    std::vector<DepartureRow> rows;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        rows.push_back(DepartureRow{
            .stop_id = text_or_empty(stmt, 0),
            .departure_time = text_or_empty(stmt, 1),
            .stop_sequence = sqlite3_column_int(stmt, 2),
            .trip_id = text_or_empty(stmt, 3),
            .route_id = text_or_empty(stmt, 4),
            .trip_headsign = optional_string(stmt, 5),
        });
    }
    return rows;
}

const std::vector<StopTimeRow>& downstream_stop_times(
    PlannerContext& ctx,
    const std::string& trip_id,
    int after_sequence
) {
    const std::string cache_key = trip_id + ":" + std::to_string(after_sequence);
    if (const auto cached = ctx.downstream_cache.find(cache_key); cached != ctx.downstream_cache.end()) {
        return cached->second;
    }
    static const std::string kSql =
        "SELECT stop_id, arrival_time, departure_time, stop_sequence "
        "FROM stop_times WHERE trip_id = ? AND stop_sequence > ? "
        "ORDER BY stop_sequence ASC";
    auto* stmt = ctx.stmts->prepare(kSql);
    sqlite3_bind_text(stmt, 1, trip_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, after_sequence);
    auto& rows = ctx.downstream_cache[cache_key];
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        rows.push_back(StopTimeRow{
            .stop_id = text_or_empty(stmt, 0),
            .arrival_time = text_or_empty(stmt, 1),
            .departure_time = text_or_empty(stmt, 2),
            .stop_sequence = sqlite3_column_int(stmt, 3),
        });
    }
    return rows;
}

const std::vector<std::pair<StopRecord, double>>& transfer_stops(
    PlannerContext& ctx,
    const std::string& stop_id,
    int radius_m,
    std::size_t limit
) {
    const std::string cache_key =
        stop_id + ":" + std::to_string(radius_m) + ":" + std::to_string(limit);
    if (const auto cached = ctx.transfer_stop_cache.find(cache_key);
        cached != ctx.transfer_stop_cache.end()) {
        return cached->second;
    }
    const auto origin_stop = fetch_stop(ctx, stop_id);
    if (!origin_stop) {
        static const std::vector<std::pair<StopRecord, double>> kEmpty;
        return kEmpty;
    }
    auto nearby = nearby_stops(ctx, origin_stop->lat, origin_stop->lon, radius_m, limit);
    std::unordered_set<std::string> seen;
    auto& deduped = ctx.transfer_stop_cache[cache_key];
    for (const auto& [candidate, distance] : nearby) {
        if (seen.contains(candidate.stop_id)) {
            continue;
        }
        seen.insert(candidate.stop_id);
        deduped.emplace_back(candidate, distance);
    }
    if (!seen.contains(origin_stop->stop_id)) {
        deduped.insert(deduped.begin(), {*origin_stop, 0.0});
    }
    if (deduped.size() > limit) {
        deduped.resize(limit);
    }
    return deduped;
}

std::optional<StopCandidate> candidate_for_stop(
    PlannerContext& ctx,
    const LocationInput& location,
    const std::string& stop_id
) {
    const auto stop = fetch_stop(ctx, stop_id);
    if (!stop) {
        return std::nullopt;
    }
    const double walk_meters = distance_to_stop(location, *stop);
    return StopCandidate{
        .stop_id = stop->stop_id,
        .stop_name = stop->stop_name,
        .lat = stop->lat,
        .lon = stop->lon,
        .walk_meters = walk_meters,
        .walk_seconds = walk_seconds(walk_meters),
    };
}

std::optional<DestinationMatch> best_destination_match(
    const std::vector<StopTimeRow>& downstream,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id,
    long long service_day_midnight_ts
) {
    std::optional<DestinationMatch> best;
    long long best_total_arrival = 0;
    for (const auto& row : downstream) {
        const auto destination = destination_by_id.find(row.stop_id);
        if (destination == destination_by_id.end()) {
            continue;
        }
        const long long arrival_ts = gtfs_time_to_timestamp(service_day_midnight_ts, row.arrival_time);
        const long long total_arrival = arrival_ts + destination->second.walk_seconds;
        if (!best || total_arrival < best_total_arrival) {
            best = DestinationMatch{
                .row = row,
                .candidate = destination->second,
                .arrival_ts = arrival_ts,
            };
            best_total_arrival = total_arrival;
        }
    }
    return best;
}

double min_destination_distance_m(
    const StopRecord& stop,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id
) {
    double best = std::numeric_limits<double>::infinity();
    for (const auto& [stop_id, destination] : destination_by_id) {
        (void)stop_id;
        best = std::min(
            best,
            haversine_m(
                stop.lat,
                stop.lon,
                destination.lat,
                destination.lon
            )
        );
    }
    return best;
}

std::vector<StopTimeRow> select_transfer_rows(const std::vector<StopTimeRow>& downstream) {
    if (downstream.size() <= static_cast<std::size_t>(kMaxIntermediateStops)) {
        return downstream;
    }
    const std::size_t step = std::max<std::size_t>(1, downstream.size() / static_cast<std::size_t>(kMaxIntermediateStops - 4));
    std::vector<StopTimeRow> sampled;
    for (std::size_t index = 0; index < downstream.size() && sampled.size() < static_cast<std::size_t>(kMaxIntermediateStops - 4); index += step) {
        sampled.push_back(downstream[index]);
    }
    for (std::size_t index = downstream.size() > 4 ? downstream.size() - 4 : 0; index < downstream.size(); ++index) {
        sampled.push_back(downstream[index]);
    }
    std::unordered_set<int> seen_sequences;
    std::vector<StopTimeRow> deduped;
    for (const auto& row : sampled) {
        if (seen_sequences.contains(row.stop_sequence)) {
            continue;
        }
        seen_sequences.insert(row.stop_sequence);
        deduped.push_back(row);
    }
    return deduped;
}

std::vector<StopTimeRow> prioritize_transfer_rows(
    PlannerContext& ctx,
    const std::vector<StopTimeRow>& transfer_rows,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id,
    std::size_t limit
) {
    if (transfer_rows.size() <= limit || destination_by_id.empty()) {
        if (transfer_rows.size() <= limit) {
            return transfer_rows;
        }
        return {
            transfer_rows.begin(),
            transfer_rows.begin() + static_cast<std::ptrdiff_t>(limit),
        };
    }

    struct RankedTransferRow {
        StopTimeRow row;
        double destination_distance_m = std::numeric_limits<double>::infinity();
    };

    std::vector<RankedTransferRow> ranked;
    ranked.reserve(transfer_rows.size());
    for (const auto& row : transfer_rows) {
        const auto stop = fetch_stop(ctx, row.stop_id);
        ranked.push_back(RankedTransferRow{
            .row = row,
            .destination_distance_m = stop
                ? min_destination_distance_m(*stop, destination_by_id)
                : std::numeric_limits<double>::infinity(),
        });
    }

    std::sort(ranked.begin(), ranked.end(), [](const RankedTransferRow& left, const RankedTransferRow& right) {
        return std::tie(
                   left.destination_distance_m,
                   left.row.stop_sequence,
                   left.row.stop_id
               ) < std::tie(
                   right.destination_distance_m,
                   right.row.stop_sequence,
                   right.row.stop_id
               );
    });
    if (ranked.size() > limit) {
        ranked.resize(limit);
    }

    std::sort(ranked.begin(), ranked.end(), [](const RankedTransferRow& left, const RankedTransferRow& right) {
        return std::tie(
                   left.row.stop_sequence,
                   left.destination_distance_m,
                   left.row.stop_id
               ) < std::tie(
                   right.row.stop_sequence,
                   right.destination_distance_m,
                   right.row.stop_id
               );
    });

    std::vector<StopTimeRow> prioritized;
    prioritized.reserve(ranked.size());
    for (const auto& entry : ranked) {
        prioritized.push_back(entry.row);
    }
    return prioritized;
}

TransitLeg make_walk_leg(
    const std::string& board_name,
    const std::string& alight_name,
    const std::string& board_id,
    const std::string& alight_id,
    long long departure_ts,
    long long arrival_ts,
    double walk_meters_value
) {
    return TransitLeg{
        .mode = "walk",
        .route_id = "walk",
        .route_name = "Walk",
        .color_hex = std::nullopt,
        .headsign = std::nullopt,
        .trip_id = std::nullopt,
        .board_stop_id = board_id,
        .board_stop_name = board_name,
        .alight_stop_id = alight_id,
        .alight_stop_name = alight_name,
        .departure_ts = departure_ts,
        .arrival_ts = arrival_ts,
        .duration_s = static_cast<int>(arrival_ts - departure_ts),
        .stop_count = 0,
        .walk_meters = walk_meters_value,
    };
}

Itinerary finalize_itinerary(std::vector<TransitLeg> legs) {
    const long long leave_at_ts = legs.front().departure_ts;
    const long long arrive_at_ts = legs.back().arrival_ts;
    const int total_duration_s = static_cast<int>(arrive_at_ts - leave_at_ts);
    int in_vehicle_s = 0;
    int walking_s = 0;
    int transit_legs = 0;
    double walk_meters_value = 0.0;
    std::vector<std::string> summary_parts;
    summary_parts.reserve(legs.size());
    for (const auto& leg : legs) {
        if (leg.mode == "walk") {
            walking_s += leg.duration_s;
            summary_parts.push_back("Walk " + std::to_string(static_cast<int>(std::round(leg.walk_meters))) + "m");
        } else {
            in_vehicle_s += leg.duration_s;
            ++transit_legs;
            summary_parts.push_back(leg.route_name);
        }
        walk_meters_value += leg.walk_meters;
    }
    const int transfer_count = std::max(0, transit_legs - 1);
    const int waiting_s = std::max(0, total_duration_s - in_vehicle_s - walking_s);
    std::ostringstream summary;
    for (std::size_t index = 0; index < summary_parts.size(); ++index) {
        if (index > 0) {
            summary << " -> ";
        }
        summary << summary_parts[index];
    }
    const std::string summary_value = summary.str();
    return Itinerary{
        .itinerary_id = std::to_string(leave_at_ts) + "-" + std::to_string(arrive_at_ts) + "-" + summary_value,
        .leave_at_ts = leave_at_ts,
        .arrive_at_ts = arrive_at_ts,
        .total_duration_s = total_duration_s,
        .in_vehicle_s = in_vehicle_s,
        .walking_s = walking_s,
        .waiting_s = waiting_s,
        .transfer_count = transfer_count,
        .walk_meters = walk_meters_value,
        .score = static_cast<double>(arrive_at_ts + transfer_count * 300 + static_cast<int>(walk_meters_value)),
        .summary = summary_value,
        .legs = std::move(legs),
    };
}

std::string itinerary_key(const Itinerary& itinerary) {
    std::ostringstream stream;
    for (const auto& leg : itinerary.legs) {
        if (leg.mode == "walk") {
            stream << "walk:" << leg.board_stop_id << ":" << leg.alight_stop_id;
        } else {
            stream << leg.route_id;
        }
        stream << "|";
    }
    stream << itinerary.leave_at_ts << "|" << itinerary.arrive_at_ts;
    return stream.str();
}

bool departure_result_less(const Itinerary& left, const Itinerary& right) {
    return std::make_tuple(
               left.arrive_at_ts,
               left.transfer_count,
               left.walk_meters
           ) < std::make_tuple(
               right.arrive_at_ts,
               right.transfer_count,
               right.walk_meters
           );
}

std::size_t search_result_cap(const PlanRequest& request) {
    return std::max<std::size_t>(
        static_cast<std::size_t>(std::max(request.num_itineraries, 1) * 4),
        12
    );
}

void trim_search_results(const PlanRequest& request, std::vector<Itinerary>& results) {
    if (request.arrive_by_ts) {
        return;
    }
    std::sort(results.begin(), results.end(), departure_result_less);
    const auto cap = search_result_cap(request);
    if (results.size() > cap) {
        results.resize(cap);
    }
}

std::optional<long long> arrival_cutoff_ts(
    const PlanRequest& request,
    const std::vector<Itinerary>& results
) {
    if (request.arrive_by_ts) {
        return std::nullopt;
    }
    const auto target_count = static_cast<std::size_t>(std::max(request.num_itineraries, 1));
    if (results.size() < target_count) {
        return std::nullopt;
    }
    // Add tolerance so slower-but-valid routes (e.g. bus+transfer)
    // aren't pruned before being explored.
    return results[target_count - 1].arrive_at_ts + kPruningToleranceS;
}

bool should_prune_arrival(
    const PlanRequest& request,
    const std::vector<Itinerary>& results,
    long long earliest_possible_arrival_ts
) {
    const auto cutoff = arrival_cutoff_ts(request, results);
    return cutoff && earliest_possible_arrival_ts >= *cutoff;
}

void maybe_record_itinerary(
    const PlanRequest& request,
    Itinerary itinerary,
    std::vector<Itinerary>& results,
    std::unordered_set<std::string>& seen
) {
    const std::string key = itinerary_key(itinerary);
    if (seen.contains(key)) {
        return;
    }
    seen.insert(key);
    results.push_back(std::move(itinerary));
    trim_search_results(request, results);
}

TransitLeg make_transit_leg(
    const RouteMeta& route_meta,
    const DepartureRow& departure,
    const StopRecord& board_stop,
    const StopTimeRow& alight_row,
    const StopRecord& alight_stop,
    long long service_day_midnight_ts
) {
    RouteMeta resolved_route = route_meta;
    const bool subway_stop_pair =
        looks_like_subway_stop_id(board_stop.stop_id) || looks_like_subway_stop_id(alight_stop.stop_id);
    if (subway_stop_pair && route_meta.mode != "subway") {
        resolved_route.mode = "subway";
        resolved_route.route_name = departure.route_id;
        resolved_route.route_long_name = departure.route_id;
        resolved_route.route_type = 1;
        resolved_route.color_hex = subway_color_for_route(departure.route_id);
    }
    const long long departure_ts = gtfs_time_to_timestamp(
        service_day_midnight_ts,
        departure.departure_time
    );
    const long long arrival_ts = gtfs_time_to_timestamp(
        service_day_midnight_ts,
        alight_row.arrival_time
    );
    return TransitLeg{
        .mode = resolved_route.mode,
        .route_id = resolved_route.route_id,
        .route_name = resolved_route.route_name,
        .color_hex = resolved_route.color_hex,
        .headsign = departure.trip_headsign ? departure.trip_headsign : std::optional<std::string>(resolved_route.route_long_name),
        .trip_id = departure.trip_id,
        .board_stop_id = board_stop.stop_id,
        .board_stop_name = board_stop.stop_name,
        .alight_stop_id = alight_stop.stop_id,
        .alight_stop_name = alight_stop.stop_name,
        .departure_ts = departure_ts,
        .arrival_ts = arrival_ts,
        .duration_s = static_cast<int>(arrival_ts - departure_ts),
        .stop_count = alight_row.stop_sequence - departure.stop_sequence,
        .walk_meters = 0.0,
    };
}

std::optional<std::vector<TransitLeg>> access_prefix_legs(
    PlannerContext& ctx,
    const PlanRequest& request,
    const DepartureRow& departure
) {
    const auto origin_candidate = candidate_for_stop(ctx, request.origin, departure.stop_id);
    const auto board_stop = fetch_stop(ctx, departure.stop_id);
    if (!origin_candidate || !board_stop) {
        return std::nullopt;
    }
    const long long departure_ts = gtfs_time_to_timestamp(
        request.service_day_midnight_ts,
        departure.departure_time
    );
    const long long leave_at_ts = departure_ts - origin_candidate->walk_seconds;
    std::vector<TransitLeg> prefix;
    if (origin_candidate->walk_seconds > 0) {
        prefix.push_back(make_walk_leg(
            request.origin.label.empty() ? "Origin" : request.origin.label,
            board_stop->stop_name,
            "origin",
            board_stop->stop_id,
            leave_at_ts,
            departure_ts,
            origin_candidate->walk_meters
        ));
    }
    return prefix;
}

std::optional<Itinerary> build_prefixed_direct_itinerary(
    PlannerContext& ctx,
    const PlanRequest& request,
    const DepartureRow& departure,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id,
    const std::vector<TransitLeg>& prefix_legs
) {
    const auto downstream = downstream_stop_times(ctx, departure.trip_id, departure.stop_sequence);
    const auto destination_match = best_destination_match(
        downstream,
        destination_by_id,
        request.service_day_midnight_ts
    );
    if (!destination_match) {
        return std::nullopt;
    }
    const auto board_stop = fetch_stop(ctx, departure.stop_id);
    const auto alight_stop = fetch_stop(ctx, destination_match->row.stop_id);
    if (!board_stop || !alight_stop) {
        return std::nullopt;
    }
    const RouteMeta route_meta = fetch_route(ctx, departure.route_id);
    const long long final_arrival_ts = destination_match->arrival_ts + destination_match->candidate.walk_seconds;

    std::vector<TransitLeg> legs = prefix_legs;
    legs.push_back(make_transit_leg(
        route_meta,
        departure,
        *board_stop,
        destination_match->row,
        *alight_stop,
        request.service_day_midnight_ts
    ));
    if (destination_match->candidate.walk_seconds > 0) {
        legs.push_back(make_walk_leg(
            alight_stop->stop_name,
            request.destination.label.empty() ? "Destination" : request.destination.label,
            alight_stop->stop_id,
            "destination",
            destination_match->arrival_ts,
            final_arrival_ts,
            destination_match->candidate.walk_meters
        ));
    }
    return finalize_itinerary(std::move(legs));
}

std::optional<Itinerary> build_direct_itinerary(
    PlannerContext& ctx,
    const PlanRequest& request,
    const DepartureRow& departure,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id
) {
    const auto prefix_legs = access_prefix_legs(ctx, request, departure);
    if (!prefix_legs) {
        return std::nullopt;
    }
    return build_prefixed_direct_itinerary(
        ctx,
        request,
        departure,
        destination_by_id,
        *prefix_legs
    );
}

void collect_transfer_itineraries(
    PlannerContext& ctx,
    const PlanRequest& request,
    const DepartureRow& departure,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id,
    const std::vector<std::string>& active_services,
    const std::vector<TransitLeg>& prefix_legs,
    int transfers_used,
    const std::unordered_set<std::string>& used_trip_ids,
    const std::optional<std::string>& previous_route_id,
    std::vector<Itinerary>& results,
    std::unordered_set<std::string>& seen
) {
    if (used_trip_ids.contains(departure.trip_id)) {
        return;
    }

    const long long departure_ts = gtfs_time_to_timestamp(
        request.service_day_midnight_ts,
        departure.departure_time
    );
    if (should_prune_arrival(request, results, departure_ts)) {
        return;
    }

    const RouteMeta route_meta = fetch_route(ctx, departure.route_id);
    if (!request.modes.contains(route_meta.mode)) {
        return;
    }
    if (previous_route_id && route_meta.route_id == *previous_route_id) {
        return;
    }

    if (const auto direct = build_prefixed_direct_itinerary(
            ctx,
            request,
            departure,
            destination_by_id,
            prefix_legs
        )) {
        maybe_record_itinerary(request, *direct, results, seen);
    }
    if (transfers_used >= request.max_transfers || seen.size() >= kMaxGeneratedItineraries) {
        return;
    }

    const auto board_stop = fetch_stop(ctx, departure.stop_id);
    if (!board_stop) {
        return;
    }
    const auto& downstream = downstream_stop_times(ctx, departure.trip_id, departure.stop_sequence);
    const auto transfer_rows = prioritize_transfer_rows(
        ctx,
        select_transfer_rows(downstream),
        destination_by_id,
        10
    );
    auto next_used_trip_ids = used_trip_ids;
    next_used_trip_ids.insert(departure.trip_id);

    for (const auto& transfer_row : transfer_rows) {
        if (seen.size() >= kMaxGeneratedItineraries) {
            break;
        }
        if (destination_by_id.contains(transfer_row.stop_id)) {
            continue;
        }
        const auto transfer_stop = fetch_stop(ctx, transfer_row.stop_id);
        if (!transfer_stop) {
            continue;
        }
        const long long transfer_arrival_ts = gtfs_time_to_timestamp(
            request.service_day_midnight_ts,
            transfer_row.arrival_time
        );
        if (should_prune_arrival(request, results, transfer_arrival_ts)) {
            continue;
        }
        const auto current_transit_leg = make_transit_leg(
            route_meta,
            departure,
            *board_stop,
            transfer_row,
            *transfer_stop,
            request.service_day_midnight_ts
        );
        const auto transfer_options = transfer_stops(
            ctx,
            transfer_stop->stop_id,
            request.max_transfer_walk_m,
            kTransferStopLimit
        );
        for (const auto& [transfer_board_stop, transfer_walk_m] : transfer_options) {
            if (seen.size() >= kMaxGeneratedItineraries) {
                break;
            }
            const int transfer_walk_seconds = walk_seconds(transfer_walk_m);
            const long long earliest_next_departure =
                transfer_arrival_ts + transfer_walk_seconds + kMinTransferSeconds;
            if (should_prune_arrival(request, results, earliest_next_departure)) {
                continue;
            }
            const auto next_departures = find_departures(
                ctx,
                {transfer_board_stop.stop_id},
                active_services,
                seconds_to_gtfs_time(earliest_next_departure - request.service_day_midnight_ts),
                seconds_to_gtfs_time(
                    request.query_ts + request.search_window_minutes * 60LL - request.service_day_midnight_ts
                ),
                kSecondLegDepartureLimit
            );

            std::vector<TransitLeg> next_prefix = prefix_legs;
            next_prefix.push_back(current_transit_leg);
            if (transfer_walk_seconds > 0) {
                next_prefix.push_back(make_walk_leg(
                    transfer_stop->stop_name,
                    transfer_board_stop.stop_name,
                    transfer_stop->stop_id,
                    transfer_board_stop.stop_id,
                    transfer_arrival_ts,
                    transfer_arrival_ts + transfer_walk_seconds,
                    transfer_walk_m
                ));
            }

            for (const auto& next_departure : next_departures) {
                const long long next_departure_ts = gtfs_time_to_timestamp(
                    request.service_day_midnight_ts,
                    next_departure.departure_time
                );
                if (should_prune_arrival(request, results, next_departure_ts)) {
                    break;
                }
                collect_transfer_itineraries(
                    ctx,
                    request,
                    next_departure,
                    destination_by_id,
                    active_services,
                    next_prefix,
                    transfers_used + 1,
                    next_used_trip_ids,
                    route_meta.route_id,
                    results,
                    seen
                );
            }
        }
    }
}

void build_transfer_itineraries(
    PlannerContext& ctx,
    const PlanRequest& request,
    const DepartureRow& departure,
    const std::unordered_map<std::string, StopCandidate>& destination_by_id,
    const std::vector<std::string>& active_services,
    std::vector<Itinerary>& results,
    std::unordered_set<std::string>& seen
) {
    const auto prefix_legs = access_prefix_legs(ctx, request, departure);
    if (!prefix_legs || request.max_transfers <= 0) {
        return;
    }
    const RouteMeta first_route = fetch_route(ctx, departure.route_id);
    const auto board_stop = fetch_stop(ctx, departure.stop_id);
    if (!board_stop) {
        return;
    }
    const auto& downstream = downstream_stop_times(ctx, departure.trip_id, departure.stop_sequence);
    const auto transfer_rows = prioritize_transfer_rows(
        ctx,
        select_transfer_rows(downstream),
        destination_by_id,
        10
    );
    for (const auto& transfer_row : transfer_rows) {
        if (destination_by_id.contains(transfer_row.stop_id)) {
            continue;
        }
        const auto transfer_stop = fetch_stop(ctx, transfer_row.stop_id);
        if (!transfer_stop) {
            continue;
        }
        const long long transfer_arrival_ts = gtfs_time_to_timestamp(
            request.service_day_midnight_ts,
            transfer_row.arrival_time
        );
        if (should_prune_arrival(request, results, transfer_arrival_ts)) {
            continue;
        }
        const auto first_transit_leg = make_transit_leg(
            first_route,
            departure,
            *board_stop,
            transfer_row,
            *transfer_stop,
            request.service_day_midnight_ts
        );
        const auto transfer_options = transfer_stops(
            ctx,
            transfer_stop->stop_id,
            request.max_transfer_walk_m,
            kTransferStopLimit
        );
        for (const auto& [transfer_board_stop, transfer_walk_m] : transfer_options) {
            const int transfer_walk_seconds = walk_seconds(transfer_walk_m);
            const long long earliest_second_departure =
                transfer_arrival_ts + transfer_walk_seconds + kMinTransferSeconds;
            if (should_prune_arrival(request, results, earliest_second_departure)) {
                continue;
            }
            const auto second_departures = find_departures(
                ctx,
                {transfer_board_stop.stop_id},
                active_services,
                seconds_to_gtfs_time(earliest_second_departure - request.service_day_midnight_ts),
                seconds_to_gtfs_time(
                    request.query_ts + request.search_window_minutes * 60LL - request.service_day_midnight_ts
                ),
                kSecondLegDepartureLimit
            );

            std::vector<TransitLeg> next_prefix = *prefix_legs;
            next_prefix.push_back(first_transit_leg);
            if (transfer_walk_seconds > 0) {
                next_prefix.push_back(make_walk_leg(
                    transfer_stop->stop_name,
                    transfer_board_stop.stop_name,
                    transfer_stop->stop_id,
                    transfer_board_stop.stop_id,
                    transfer_arrival_ts,
                    transfer_arrival_ts + transfer_walk_seconds,
                    transfer_walk_m
                ));
            }

            for (const auto& second_departure : second_departures) {
                const long long second_departure_ts = gtfs_time_to_timestamp(
                    request.service_day_midnight_ts,
                    second_departure.departure_time
                );
                if (should_prune_arrival(request, results, second_departure_ts)) {
                    break;
                }
                collect_transfer_itineraries(
                    ctx,
                    request,
                    second_departure,
                    destination_by_id,
                    active_services,
                    next_prefix,
                    1,
                    {departure.trip_id},
                    first_route.route_id,
                    results,
                    seen
                );
            }
        }
    }
}

LocationInput parse_location(const nlohmann::json& payload) {
    return LocationInput{
        .label = payload.value("label", ""),
        .lat = optional_double(payload, "lat"),
        .lon = optional_double(payload, "lon"),
        .stop_id = optional_json_string(payload, "stop_id"),
        .address = optional_json_string(payload, "address"),
    };
}

long long current_timestamp_s() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string format_duration_label(int total_seconds) {
    const int minutes = std::max(1, static_cast<int>(std::round(total_seconds / 60.0)));
    const int hours = minutes / 60;
    const int mins = minutes % 60;
    if (hours <= 0) {
        return std::to_string(mins) + " min";
    }
    std::ostringstream stream;
    stream << hours << "h ";
    stream.fill('0');
    stream.width(2);
    stream << mins << "m";
    return stream.str();
}

std::string format_clock_label(long long timestamp_s, long long service_day_midnight_ts) {
    long long seconds_since_midnight = timestamp_s - service_day_midnight_ts;
    if (seconds_since_midnight < 0) {
        seconds_since_midnight = 0;
    }
    seconds_since_midnight %= 24LL * 3600LL;
    const int hour24 = static_cast<int>((seconds_since_midnight / 3600LL) % 24LL);
    const int minute = static_cast<int>((seconds_since_midnight % 3600LL) / 60LL);
    const int hour12 = hour24 % 12 == 0 ? 12 : hour24 % 12;
    std::ostringstream stream;
    stream << hour12 << ":";
    stream.fill('0');
    stream.width(2);
    stream << minute << (hour24 < 12 ? " AM" : " PM");
    return stream.str();
}

std::string walk_chip_label(int duration_s) {
    return "Walk " + std::to_string(std::max(1, static_cast<int>(std::round(duration_s / 60.0)))) + " min";
}

nlohmann::json location_to_json(const LocationInput& location) {
    return {
        {"label", location.label},
        {"lat", location.lat ? nlohmann::json(*location.lat) : nlohmann::json(nullptr)},
        {"lon", location.lon ? nlohmann::json(*location.lon) : nlohmann::json(nullptr)},
        {"stop_id", location.stop_id ? nlohmann::json(*location.stop_id) : nlohmann::json(nullptr)},
        {"address", location.address ? nlohmann::json(*location.address) : nlohmann::json(nullptr)},
    };
}

nlohmann::json route_chips_to_json(const Itinerary& itinerary) {
    nlohmann::json chips = nlohmann::json::array();
    for (const auto& leg : itinerary.legs) {
        if (leg.mode == "walk") {
            chips.push_back({
                {"kind", "walk"},
                {"label", walk_chip_label(leg.duration_s)},
                {"route_id", nullptr},
                {"color_hex", nullptr},
                {"mode", "walk"},
                {"duration_s", leg.duration_s},
                {"walk_meters", leg.walk_meters},
            });
            continue;
        }
        chips.push_back({
            {"kind", "transit"},
            {"label", leg.route_name},
            {"route_id", leg.route_id},
            {"color_hex", leg.color_hex ? nlohmann::json(*leg.color_hex) : nlohmann::json(nullptr)},
            {"mode", leg.mode},
            {"duration_s", leg.duration_s},
            {"walk_meters", nullptr},
        });
    }
    return chips;
}

nlohmann::json go_steps_to_json(
    const Itinerary& itinerary,
    const LocationInput& origin,
    const LocationInput& destination,
    long long service_day_midnight_ts
) {
    nlohmann::json steps = nlohmann::json::array();
    for (std::size_t index = 0; index < itinerary.legs.size(); ++index) {
        const auto& leg = itinerary.legs[index];
        if (leg.mode == "walk") {
            std::string title;
            if (index == 0) {
                title = "Walk from " + origin.label + " to " + leg.alight_stop_name;
            } else if (index == itinerary.legs.size() - 1) {
                title = "Walk to " + destination.label;
            } else {
                title = "Walk transfer to " + leg.alight_stop_name;
            }
            steps.push_back({
                {"kind", "walk"},
                {"title", title},
                {"subtitle", format_duration_label(leg.duration_s) + " • " +
                                 std::to_string(static_cast<int>(std::round(leg.walk_meters))) + "m"},
                {"start_ts", leg.departure_ts},
                {"end_ts", leg.arrival_ts},
                {"route_id", leg.route_id},
                {"route_name", leg.route_name},
                {"color_hex", nullptr},
                {"stop_id", leg.alight_stop_id},
                {"stop_name", leg.alight_stop_name},
            });
            continue;
        }

        steps.push_back({
            {"kind", "ride"},
            {"title", "Take " + leg.route_name},
            {"subtitle", "Board at " + leg.board_stop_name + " toward " +
                             (leg.headsign ? *leg.headsign : leg.alight_stop_name) + " • " +
                             format_duration_label(leg.duration_s)},
            {"start_ts", leg.departure_ts},
            {"end_ts", leg.arrival_ts},
            {"route_id", leg.route_id},
            {"route_name", leg.route_name},
            {"color_hex", leg.color_hex ? nlohmann::json(*leg.color_hex) : nlohmann::json(nullptr)},
            {"stop_id", leg.board_stop_id},
            {"stop_name", leg.board_stop_name},
        });
    }

    const std::string final_stop_id =
        itinerary.legs.empty() ? std::string() : itinerary.legs.back().alight_stop_id;
    steps.push_back({
        {"kind", "arrive"},
        {"title", "Arrive at " + destination.label},
        {"subtitle", "Arrival at " + format_clock_label(itinerary.arrive_at_ts, service_day_midnight_ts)},
        {"start_ts", itinerary.arrive_at_ts},
        {"end_ts", itinerary.arrive_at_ts},
        {"route_id", nullptr},
        {"route_name", nullptr},
        {"color_hex", nullptr},
        {"stop_id", final_stop_id.empty() ? nlohmann::json(nullptr) : nlohmann::json(final_stop_id)},
        {"stop_name", destination.label},
    });
    return steps;
}

nlohmann::json go_transfers_to_json(const Itinerary& itinerary) {
    std::vector<std::size_t> transit_indexes;
    for (std::size_t index = 0; index < itinerary.legs.size(); ++index) {
        if (itinerary.legs[index].mode != "walk") {
            transit_indexes.push_back(index);
        }
    }
    nlohmann::json transfers = nlohmann::json::array();
    for (std::size_t pair_index = 1; pair_index < transit_indexes.size(); ++pair_index) {
        const auto& incoming = itinerary.legs[transit_indexes[pair_index - 1]];
        const auto& outgoing = itinerary.legs[transit_indexes[pair_index]];
        int walk_s = 0;
        double walk_meters_value = 0.0;
        for (std::size_t leg_index = transit_indexes[pair_index - 1] + 1;
             leg_index < transit_indexes[pair_index];
             ++leg_index) {
            const auto& leg = itinerary.legs[leg_index];
            if (leg.mode == "walk") {
                walk_s += leg.duration_s;
                walk_meters_value += leg.walk_meters;
            }
        }
        const int wait_s = std::max(
            0,
            static_cast<int>(outgoing.departure_ts - incoming.arrival_ts) - walk_s
        );
        transfers.push_back({
            {"from_route_id", incoming.route_id},
            {"from_route_name", incoming.route_name},
            {"to_route_id", outgoing.route_id},
            {"to_route_name", outgoing.route_name},
            {"arrival_stop_id", incoming.alight_stop_id},
            {"arrival_stop_name", incoming.alight_stop_name},
            {"boarding_stop_id", outgoing.board_stop_id},
            {"boarding_stop_name", outgoing.board_stop_name},
            {"arrival_ts", incoming.arrival_ts},
            {"boarding_ts", outgoing.departure_ts},
            {"wait_s", wait_s},
            {"walk_s", walk_s},
            {"walk_meters", walk_meters_value},
        });
    }
    return transfers;
}

std::pair<std::string, nlohmann::json> next_action_to_json(
    const Itinerary& itinerary,
    const LocationInput& origin,
    const LocationInput& destination,
    long long now_ts
) {
    (void)origin;
    if (now_ts <= itinerary.leave_at_ts) {
        const int due_in_s = std::max(0, static_cast<int>(itinerary.leave_at_ts - now_ts));
        std::string subtitle = "Trip ready";
        if (!itinerary.legs.empty()) {
            const auto& first_leg = itinerary.legs.front();
            if (first_leg.mode == "walk") {
                subtitle = "Walk to " + first_leg.alight_stop_name;
            } else {
                subtitle = "Board " + first_leg.route_name + " at " + first_leg.board_stop_name;
            }
        }
        return {
            "upcoming",
            {
                {"status", "upcoming"},
                {"title", due_in_s <= 60 ? "Leave now" : "Leave in " + format_duration_label(due_in_s)},
                {"subtitle", subtitle},
                {"due_at_ts", itinerary.leave_at_ts},
                {"due_in_s", due_in_s},
            },
        };
    }

    for (const auto& leg : itinerary.legs) {
        if (leg.departure_ts <= now_ts && now_ts < leg.arrival_ts) {
            if (leg.mode == "walk") {
                return {
                    "walking",
                    {
                        {"status", "walking"},
                        {"title", "Walk to " + leg.alight_stop_name},
                        {"subtitle", format_duration_label(std::max(0, static_cast<int>(leg.arrival_ts - now_ts))) + " remaining"},
                        {"due_at_ts", leg.arrival_ts},
                        {"due_in_s", std::max(0, static_cast<int>(leg.arrival_ts - now_ts))},
                    },
                };
            }
            return {
                "riding",
                {
                    {"status", "riding"},
                    {"title", "Ride " + leg.route_name},
                    {"subtitle", "Stay on until " + leg.alight_stop_name},
                    {"due_at_ts", leg.arrival_ts},
                    {"due_in_s", std::max(0, static_cast<int>(leg.arrival_ts - now_ts))},
                },
            };
        }
        if (now_ts < leg.departure_ts) {
            if (leg.mode == "walk") {
                return {
                    "waiting",
                    {
                        {"status", "waiting"},
                        {"title", "Start walking"},
                        {"subtitle", "Walk to " + leg.alight_stop_name},
                        {"due_at_ts", leg.departure_ts},
                        {"due_in_s", std::max(0, static_cast<int>(leg.departure_ts - now_ts))},
                    },
                };
            }
            return {
                "waiting",
                {
                    {"status", "waiting"},
                    {"title", "Board " + leg.route_name},
                    {"subtitle", "At " + leg.board_stop_name + " toward " +
                                     (leg.headsign ? *leg.headsign : leg.alight_stop_name)},
                    {"due_at_ts", leg.departure_ts},
                    {"due_in_s", std::max(0, static_cast<int>(leg.departure_ts - now_ts))},
                },
            };
        }
    }

    return {
        "arrived",
        {
            {"status", "arrived"},
            {"title", "Arrive at " + destination.label},
            {"subtitle", "Trip complete"},
            {"due_at_ts", itinerary.arrive_at_ts},
            {"due_in_s", 0},
        },
    };
}

std::string session_kind(const PlanRequest& request, long long now_ts) {
    if (request.arrive_by_ts) {
        return "arrive_by";
    }
    if (request.depart_at_ts && *request.depart_at_ts > now_ts + 60) {
        return "depart_at";
    }
    return "leave_now";
}

nlohmann::json go_trip_to_json(
    const Itinerary& itinerary,
    const PlanRequest& request,
    long long now_ts
) {
    const auto [status, next_action] = next_action_to_json(
        itinerary,
        request.origin,
        request.destination,
        now_ts
    );
    return {
        {"itinerary", itinerary_to_json(itinerary)},
        {"route_chips", route_chips_to_json(itinerary)},
        {"steps", go_steps_to_json(
            itinerary,
            request.origin,
            request.destination,
            request.service_day_midnight_ts
        )},
        {"transfers", go_transfers_to_json(itinerary)},
        {"next_action", next_action},
        {"status", status},
        {"leave_in_s", std::max(0, static_cast<int>(itinerary.leave_at_ts - now_ts))},
        {"arrive_in_s", std::max(0, static_cast<int>(itinerary.arrive_at_ts - now_ts))},
        {"duration_label", format_duration_label(itinerary.total_duration_s)},
        {"leave_label", format_clock_label(itinerary.leave_at_ts, request.service_day_midnight_ts)},
        {"arrive_label", format_clock_label(itinerary.arrive_at_ts, request.service_day_midnight_ts)},
    };
}

}  // namespace

EngineService::EngineService(std::string schedule_db_path)
    : schedule_db_path_(std::move(schedule_db_path)) {}

const char* EngineService::version() {
    return "0.9.0";
}

HealthStatus EngineService::health() const {
    HealthStatus health{
        .version = version(),
        .backend = "cpp",
        .schedule_db_path = schedule_db_path_,
        .ready = false,
        .indexes = {},
        .missing_indexes = {},
    };
    try {
        if (!std::filesystem::exists(schedule_db_path_)) {
            return health;
        }
        SqliteConnection connection(schedule_db_path_);
        health.ready = true;
        const auto available = existing_index_names(connection.db);
        std::unordered_set<std::string> available_set(available.begin(), available.end());
        for (const auto* index_name : kImportantIndexes) {
            if (available_set.contains(index_name)) {
                health.indexes.emplace_back(index_name);
            } else {
                health.missing_indexes.emplace_back(index_name);
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "TrackEngine health error: " << error.what() << "\n";
        health.ready = false;
    }
    return health;
}

std::vector<Itinerary> EngineService::compute_itineraries(const PlanRequest& request) const {
    SqliteConnection connection(schedule_db_path_);
    PlannerContext ctx{
        .db = connection.db,
        .stmts = std::make_unique<StatementCache>(connection.db),
    };

    const auto active_services = active_service_ids(ctx, request.service_day_yyyymmdd, request.service_weekday);
    if (active_services.empty()) {
        return {};
    }

    const auto origin_candidates = resolve_candidates(ctx, request.origin, request.max_origin_walk_m, request.modes);
    const auto destination_candidates = resolve_candidates(ctx, request.destination, request.max_destination_walk_m, request.modes);
    if (origin_candidates.empty() || destination_candidates.empty()) {
        return {};
    }

    std::unordered_map<std::string, StopCandidate> destination_by_id;
    destination_by_id.reserve(destination_candidates.size());
    for (const auto& candidate : destination_candidates) {
        destination_by_id.emplace(candidate.stop_id, candidate);
    }

    const std::string depart_from = seconds_to_gtfs_time(request.query_ts - request.service_day_midnight_ts);
    const std::string depart_to = seconds_to_gtfs_time(
        request.query_ts + request.search_window_minutes * 60LL - request.service_day_midnight_ts
    );
    std::vector<DepartureRow> departures;
    departures.reserve(origin_candidates.size() * 8);
    for (const auto& candidate : origin_candidates) {
        auto stop_departures = find_departures(
            ctx,
            {candidate.stop_id},
            active_services,
            depart_from,
            depart_to,
            12
        );
        departures.insert(
            departures.end(),
            std::make_move_iterator(stop_departures.begin()),
            std::make_move_iterator(stop_departures.end())
        );
    }
    std::sort(departures.begin(), departures.end(), [](const DepartureRow& left, const DepartureRow& right) {
        return std::tie(left.departure_time, left.stop_sequence, left.stop_id, left.trip_id) <
               std::tie(right.departure_time, right.stop_sequence, right.stop_id, right.trip_id);
    });
    // Mode-balanced truncation: keep at least some departures per mode
    // so subway frequency doesn't crowd out bus results.
    if (departures.size() > static_cast<std::size_t>(kOriginDepartureLimit)) {
        std::unordered_map<std::string, int> mode_counts;
        constexpr int kMinPerMode = 8;
        std::vector<DepartureRow> balanced;
        balanced.reserve(kOriginDepartureLimit);
        // First pass: guarantee minimum per mode
        for (const auto& dep : departures) {
            const RouteMeta rm = fetch_route(ctx, dep.route_id);
            auto& cnt = mode_counts[rm.mode];
            if (cnt < kMinPerMode) {
                balanced.push_back(dep);
                ++cnt;
            }
        }
        // Second pass: fill remaining slots in time order
        std::unordered_set<std::string> balanced_keys;
        for (const auto& b : balanced) {
            balanced_keys.insert(b.trip_id + b.stop_id);
        }
        for (const auto& dep : departures) {
            if (balanced.size() >= static_cast<std::size_t>(kOriginDepartureLimit)) break;
            if (!balanced_keys.contains(dep.trip_id + dep.stop_id)) {
                balanced.push_back(dep);
            }
        }
        std::sort(balanced.begin(), balanced.end(), [](const DepartureRow& a, const DepartureRow& b) {
            return std::tie(a.departure_time, a.stop_sequence, a.stop_id, a.trip_id) <
                   std::tie(b.departure_time, b.stop_sequence, b.stop_id, b.trip_id);
        });
        departures = std::move(balanced);
    }

    std::vector<Itinerary> itineraries;
    std::unordered_set<std::string> seen;
    // Track which requested modes have produced at least one itinerary.
    std::unordered_set<std::string> modes_with_results;

    for (const auto& departure : departures) {
        const RouteMeta route_meta = fetch_route(ctx, departure.route_id);
        if (!request.modes.contains(route_meta.mode)) {
            continue;
        }

        const long long departure_ts = gtfs_time_to_timestamp(
            request.service_day_midnight_ts,
            departure.departure_time
        );
        if (should_prune_arrival(request, itineraries, departure_ts)) {
            // If every requested mode already has results, stop.
            // Otherwise keep exploring underrepresented modes.
            if (modes_with_results.size() >= request.modes.size()) {
                break;
            }
            if (modes_with_results.contains(route_meta.mode)) {
                continue;  // this mode already represented, skip
            }
        }

        if (const auto direct = build_direct_itinerary(ctx, request, departure, destination_by_id)) {
            maybe_record_itinerary(request, *direct, itineraries, seen);
            for (const auto& leg : direct->legs) {
                if (leg.mode != "walk") modes_with_results.insert(leg.mode);
            }
        }
        if (request.max_transfers <= 0) {
            continue;
        }
        const auto pre_transfer_count = itineraries.size();
        build_transfer_itineraries(
            ctx,
            request,
            departure,
            destination_by_id,
            active_services,
            itineraries,
            seen
        );
        // Record modes from any newly added transfer itineraries.
        for (auto i = pre_transfer_count; i < itineraries.size(); ++i) {
            for (const auto& leg : itineraries[i].legs) {
                if (leg.mode != "walk") modes_with_results.insert(leg.mode);
            }
        }
    }

    // ── Second pass: supplementary search for underrepresented modes ──
    // If a requested mode (e.g. "bus") has zero itineraries after the main
    // loop, re-scan bus departures with NO pruning cutoff so that slower
    // bus→subway transfer itineraries are discovered.
    {
        std::unordered_set<std::string> missing_modes;
        for (const auto& m : request.modes) {
            if (!modes_with_results.contains(m)) {
                missing_modes.insert(m);
            }
        }
        // Quick check: count departures that belong to missing modes.
        // If zero, skip the entire supplementary search.
        bool has_missing_mode_departures = false;
        if (!missing_modes.empty()) {
            for (const auto& dep : departures) {
                const RouteMeta rm = fetch_route(ctx, dep.route_id);
                if (missing_modes.contains(rm.mode)) {
                    has_missing_mode_departures = true;
                    break;
                }
            }
        }
        if (has_missing_mode_departures) {
            // Build a relaxed request with no effective pruning: set a very
            // large search window and no arrival-based cutoff by temporarily
            // collecting into a separate result vector.
            std::vector<Itinerary> supplementary;
            std::unordered_set<std::string> sup_seen = seen;  // keep existing dedup

            for (const auto& departure : departures) {
                if (supplementary.size() >= 4) break;  // cap supplementary results
                const RouteMeta route_meta = fetch_route(ctx, departure.route_id);
                if (!missing_modes.contains(route_meta.mode)) {
                    continue;  // only explore missing modes
                }
                // Try direct itinerary (unlikely for bus→Manhattan but cheap)
                if (const auto direct = build_direct_itinerary(ctx, request, departure, destination_by_id)) {
                    const std::string key = itinerary_key(*direct);
                    if (!sup_seen.contains(key)) {
                        sup_seen.insert(key);
                        supplementary.push_back(*direct);
                    }
                }
                if (request.max_transfers <= 0) continue;

                // For transfer exploration, temporarily inflate results to
                // disable should_prune_arrival checks inside build_transfer_itineraries.
                // We do this by passing an empty results vector.
                std::vector<Itinerary> transfer_results;
                std::unordered_set<std::string> transfer_seen = sup_seen;
                build_transfer_itineraries(
                    ctx,
                    request,
                    departure,
                    destination_by_id,
                    active_services,
                    transfer_results,
                    transfer_seen
                );
                for (auto& it : transfer_results) {
                    const std::string key = itinerary_key(it);
                    if (!sup_seen.contains(key)) {
                        sup_seen.insert(key);
                        supplementary.push_back(std::move(it));
                        if (supplementary.size() >= 4) break;
                    }
                }
            }
            // Sort supplementary by arrival time, keep best 2 per missing mode
            std::sort(supplementary.begin(), supplementary.end(), departure_result_less);
            for (const auto& it : supplementary) {
                if (itineraries.size() >= static_cast<std::size_t>(request.num_itineraries) + 2) break;
                itineraries.push_back(it);
            }
        }
    }

    if (request.arrive_by_ts) {
        itineraries.erase(
            std::remove_if(itineraries.begin(), itineraries.end(), [&](const Itinerary& itinerary) {
                return itinerary.arrive_at_ts > *request.arrive_by_ts;
            }),
            itineraries.end()
        );
        std::sort(itineraries.begin(), itineraries.end(), [](const Itinerary& left, const Itinerary& right) {
            return std::make_tuple(
                       -left.leave_at_ts,
                       left.total_duration_s,
                       left.transfer_count,
                       left.walk_meters
                   ) < std::make_tuple(
                       -right.leave_at_ts,
                       right.total_duration_s,
                       right.transfer_count,
                       right.walk_meters
                   );
        });
    } else {
        std::sort(itineraries.begin(), itineraries.end(), [](const Itinerary& left, const Itinerary& right) {
            return std::make_tuple(
                       left.arrive_at_ts,
                       left.transfer_count,
                       left.walk_meters
                   ) < std::make_tuple(
                       right.arrive_at_ts,
                       right.transfer_count,
                       right.walk_meters
                   );
        });
    }
    if (itineraries.size() > static_cast<std::size_t>(request.num_itineraries)) {
        itineraries.resize(static_cast<std::size_t>(request.num_itineraries));
    }
    return itineraries;
}

nlohmann::json EngineService::plan(const PlanRequest& request) const {
    const auto itineraries = compute_itineraries(request);
    nlohmann::json payload;
    payload["engine_version"] = version();
    payload["requested_at_ts"] = current_timestamp_s();
    payload["itineraries"] = nlohmann::json::array();
    for (const auto& itinerary : itineraries) {
        payload["itineraries"].push_back(itinerary_to_json(itinerary));
    }
    return payload;
}

nlohmann::json EngineService::go(const PlanRequest& request) const {
    const auto itineraries = compute_itineraries(request);
    const long long now_ts = request.now_ts.value_or(current_timestamp_s());

    nlohmann::json payload;
    payload["engine_version"] = version();
    payload["requested_at_ts"] = current_timestamp_s();
    payload["now_ts"] = now_ts;
    payload["origin"] = location_to_json(request.origin);
    payload["destination"] = location_to_json(request.destination);
    payload["session_kind"] = session_kind(request, now_ts);
    payload["primary_trip"] = nullptr;
    payload["alternatives"] = nlohmann::json::array();

    if (!itineraries.empty()) {
        payload["primary_trip"] = go_trip_to_json(itineraries.front(), request, now_ts);
        for (std::size_t index = 1; index < itineraries.size(); ++index) {
            payload["alternatives"].push_back(go_trip_to_json(itineraries[index], request, now_ts));
        }
    }
    return payload;
}

nlohmann::json health_to_json(const HealthStatus& health) {
    return {
        {"version", health.version},
        {"backend", health.backend},
        {"schedule_db_path", health.schedule_db_path},
        {"ready", health.ready},
        {"indexes", health.indexes},
        {"missing_indexes", health.missing_indexes},
    };
}

nlohmann::json itinerary_to_json(const Itinerary& itinerary) {
    nlohmann::json legs = nlohmann::json::array();
    for (const auto& leg : itinerary.legs) {
        legs.push_back({
            {"mode", leg.mode},
            {"route_id", leg.route_id},
            {"route_name", leg.route_name},
            {"color_hex", leg.color_hex ? nlohmann::json(*leg.color_hex) : nlohmann::json(nullptr)},
            {"headsign", leg.headsign ? nlohmann::json(*leg.headsign) : nlohmann::json(nullptr)},
            {"trip_id", leg.trip_id ? nlohmann::json(*leg.trip_id) : nlohmann::json(nullptr)},
            {"board_stop_id", leg.board_stop_id},
            {"board_stop_name", leg.board_stop_name},
            {"alight_stop_id", leg.alight_stop_id},
            {"alight_stop_name", leg.alight_stop_name},
            {"departure_ts", leg.departure_ts},
            {"arrival_ts", leg.arrival_ts},
            {"duration_s", leg.duration_s},
            {"stop_count", leg.stop_count},
            {"walk_meters", leg.walk_meters},
        });
    }
    return {
        {"itinerary_id", itinerary.itinerary_id},
        {"leave_at_ts", itinerary.leave_at_ts},
        {"arrive_at_ts", itinerary.arrive_at_ts},
        {"total_duration_s", itinerary.total_duration_s},
        {"in_vehicle_s", itinerary.in_vehicle_s},
        {"walking_s", itinerary.walking_s},
        {"waiting_s", itinerary.waiting_s},
        {"transfer_count", itinerary.transfer_count},
        {"walk_meters", itinerary.walk_meters},
        {"score", itinerary.score},
        {"summary", itinerary.summary},
        {"legs", legs},
    };
}

PlanRequest plan_request_from_json(const nlohmann::json& payload) {
    PlanRequest request;
    request.origin = parse_location(payload.at("origin"));
    request.destination = parse_location(payload.at("destination"));
    request.depart_at_ts = optional_int64(payload, "depart_at_ts");
    request.arrive_by_ts = optional_int64(payload, "arrive_by_ts");
    request.now_ts = optional_int64(payload, "now_ts");
    request.query_ts = payload.value("query_ts", 0LL);
    request.service_day_midnight_ts = payload.value("service_day_midnight_ts", 0LL);
    request.service_day_yyyymmdd = payload.value("service_day_yyyymmdd", 0);
    request.service_weekday = payload.value("service_weekday", 0);
    request.max_transfers = payload.value("max_transfers", 1);
    request.max_origin_walk_m = payload.value("max_origin_walk_m", 1200);
    request.max_destination_walk_m = payload.value("max_destination_walk_m", 1200);
    request.max_transfer_walk_m = payload.value("max_transfer_walk_m", 400);
    request.search_window_minutes = payload.value("search_window_minutes", 180);
    request.num_itineraries = payload.value("num_itineraries", 3);
    request.modes.clear();
    if (payload.contains("modes") && payload.at("modes").is_array()) {
        for (const auto& mode : payload.at("modes")) {
            request.modes.insert(mode.get<std::string>());
        }
    }
    if (request.modes.empty()) {
        request.modes = {"subway", "bus", "lirr", "mnr"};
    }
    return request;
}

}  // namespace trackengine
