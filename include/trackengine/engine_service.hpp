#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"

namespace trackengine {

struct LocationInput {
    std::string label;
    std::optional<double> lat;
    std::optional<double> lon;
    std::optional<std::string> stop_id;
    std::optional<std::string> address;
};

struct PlanRequest {
    LocationInput origin;
    LocationInput destination;
    std::optional<long long> depart_at_ts;
    std::optional<long long> arrive_by_ts;
    std::optional<long long> now_ts;
    long long query_ts = 0;
    long long service_day_midnight_ts = 0;
    int service_day_yyyymmdd = 0;
    int service_weekday = 0;
    int max_transfers = 1;
    int max_origin_walk_m = 1200;
    int max_destination_walk_m = 1200;
    int max_transfer_walk_m = 250;
    int search_window_minutes = 180;
    int num_itineraries = 3;
    std::unordered_set<std::string> modes = {"subway", "bus", "lirr", "mnr"};
    std::string priority;  // "quick" (default), "fewer_transfers", "less_walking"
    bool accessibility_priority = false;
};

struct StopRecord {
    std::string stop_id;
    std::string stop_name;
    double lat = 0.0;
    double lon = 0.0;
};

struct StopCandidate {
    std::string stop_id;
    std::string stop_name;
    double lat = 0.0;
    double lon = 0.0;
    double walk_meters = 0.0;
    int walk_seconds = 0;
};

struct RouteMeta {
    std::string route_id;
    std::string route_name;
    std::string route_long_name;
    std::optional<std::string> color_hex;
    std::optional<int> route_type;
    std::string mode = "subway";
};

struct DepartureRow {
    std::string stop_id;
    std::string departure_time;
    int stop_sequence = 0;
    std::string trip_id;
    std::string route_id;
    std::optional<std::string> trip_headsign;
};

struct StopTimeRow {
    std::string stop_id;
    std::string arrival_time;
    std::string departure_time;
    int stop_sequence = 0;
};

struct TransitLeg {
    std::string mode;
    std::string route_id;
    std::string route_name;
    std::optional<std::string> color_hex;
    std::optional<std::string> headsign;
    std::optional<std::string> trip_id;
    std::string board_stop_id;
    std::string board_stop_name;
    std::string alight_stop_id;
    std::string alight_stop_name;
    long long departure_ts = 0;
    long long arrival_ts = 0;
    int duration_s = 0;
    int stop_count = 0;
    double walk_meters = 0.0;
};

struct Itinerary {
    std::string itinerary_id;
    long long leave_at_ts = 0;
    long long arrive_at_ts = 0;
    int total_duration_s = 0;
    int in_vehicle_s = 0;
    int walking_s = 0;
    int waiting_s = 0;
    int transfer_count = 0;
    double walk_meters = 0.0;
    double score = 0.0;
    std::string summary;
    std::vector<TransitLeg> legs;
};

struct HealthStatus {
    std::string version;
    std::string backend;
    std::string schedule_db_path;
    bool ready = false;
    std::vector<std::string> indexes;
    std::vector<std::string> missing_indexes;
};

class EngineService {
public:
    explicit EngineService(std::string schedule_db_path);

    HealthStatus health() const;
    nlohmann::json plan(const PlanRequest& request) const;
    nlohmann::json go(const PlanRequest& request) const;
    const std::string& schedule_db_path() const { return schedule_db_path_; }
    static const char* version();

private:
    std::vector<Itinerary> compute_itineraries(const PlanRequest& request) const;
    std::string schedule_db_path_;
};

nlohmann::json health_to_json(const HealthStatus& health);
nlohmann::json itinerary_to_json(const Itinerary& itinerary);
PlanRequest plan_request_from_json(const nlohmann::json& payload);

}  // namespace trackengine
