#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#include "httplib.h"
#include "nlohmann/json.hpp"
#include "trackengine/engine_service.hpp"
#include "trackengine/logger.hpp"

namespace {

std::string env_or(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    return (value != nullptr && value[0] != '\0') ? std::string(value) : std::string(fallback);
}

int env_int_or(const char* key, int fallback) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    try {
        return std::stoi(value);
    } catch (...) {
        return fallback;
    }
}

void json_response(httplib::Response& response, int status, const nlohmann::json& payload) {
    response.status = status;
    response.set_content(payload.dump(), "application/json");
}

}  // namespace

int main() {
    const std::string schedule_db_path = env_or(
        "TRACK_ENGINE_SCHEDULE_DB",
        "/app/data/transit_schedule.db"
    );
    const std::string host = env_or("TRACK_ENGINE_HOST", "0.0.0.0");
    const int port = env_int_or("PORT", env_int_or("TRACK_ENGINE_PORT", 8080));
    const int thread_count = std::max(2, env_int_or("TRACK_ENGINE_THREADS", 8));

    trackengine::EngineService service(schedule_db_path);
    httplib::Server server;
    server.new_task_queue = [thread_count] {
        return new httplib::ThreadPool(static_cast<std::size_t>(thread_count));
    };
    server.set_read_timeout(15, 0);
    server.set_write_timeout(30, 0);
    server.set_idle_interval(0, 200'000);

    server.Get("/", [&](const httplib::Request&, httplib::Response& response) {
        json_response(response, 200, {
            {"service", "TrackEngine"},
            {"backend", "cpp"},
            {"version", trackengine::EngineService::version()},
        });
    });

    server.Get("/health", [&](const httplib::Request&, httplib::Response& response) {
        const auto health = trackengine::health_to_json(service.health());
        const int status = health.value("ready", false) ? 200 : 503;
        json_response(response, status, health);
    });

    server.Post("/plan", [&](const httplib::Request& request, httplib::Response& response) {
        trackengine::log::Timer timer;
        try {
            const auto payload = nlohmann::json::parse(request.body);
            const auto plan_request = trackengine::plan_request_from_json(payload);
            const auto result = service.plan(plan_request);
            json_response(response, 200, result);
            trackengine::log::request("POST", "/plan", 200, timer.elapsed_ms(), {
                {"itineraries", result.value("itineraries", nlohmann::json::array()).size()},
                {"origin", plan_request.origin.label},
                {"destination", plan_request.destination.label},
            });
        } catch (const nlohmann::json::exception& error) {
            json_response(response, 400, {
                {"error", "invalid_json"},
                {"detail", error.what()},
            });
            trackengine::log::request("POST", "/plan", 400, timer.elapsed_ms(), {
                {"error", error.what()},
            });
        } catch (const std::exception& error) {
            json_response(response, 500, {
                {"error", "planner_failed"},
                {"detail", error.what()},
            });
            trackengine::log::request("POST", "/plan", 500, timer.elapsed_ms(), {
                {"error", error.what()},
            });
        }
    });

    server.Post("/go", [&](const httplib::Request& request, httplib::Response& response) {
        trackengine::log::Timer timer;
        try {
            const auto payload = nlohmann::json::parse(request.body);
            const auto plan_request = trackengine::plan_request_from_json(payload);
            const auto result = service.go(plan_request);
            const int trip_count = 1 + static_cast<int>(result.value("alternatives", nlohmann::json::array()).size());
            json_response(response, 200, result);
            trackengine::log::request("POST", "/go", 200, timer.elapsed_ms(), {
                {"trips", trip_count},
                {"origin", plan_request.origin.label},
                {"destination", plan_request.destination.label},
                {"max_transfers", plan_request.max_transfers},
            });
        } catch (const nlohmann::json::exception& error) {
            json_response(response, 400, {
                {"error", "invalid_json"},
                {"detail", error.what()},
            });
            trackengine::log::request("POST", "/go", 400, timer.elapsed_ms(), {
                {"error", error.what()},
            });
        } catch (const std::exception& error) {
            json_response(response, 500, {
                {"error", "go_builder_failed"},
                {"detail", error.what()},
            });
            trackengine::log::request("POST", "/go", 500, timer.elapsed_ms(), {
                {"error", error.what()},
            });
        }
    });

    server.set_exception_handler([](const httplib::Request&, httplib::Response& response, std::exception_ptr error_ptr) {
        try {
            if (error_ptr != nullptr) {
                std::rethrow_exception(error_ptr);
            }
        } catch (const std::exception& error) {
            json_response(response, 500, {
                {"error", "server_exception"},
                {"detail", error.what()},
            });
            return;
        }
        json_response(response, 500, {
            {"error", "server_exception"},
            {"detail", "unknown error"},
        });
    });

    server.set_error_handler([](const httplib::Request& req, httplib::Response& response) {
        if (!response.body.empty()) {
            return;
        }
        json_response(response, response.status, {
            {"error", "not_found"},
            {"status", response.status},
        });
        trackengine::log::warn("HTTP", "Not found", {
            {"path", req.path},
            {"status", response.status},
        });
    });

    const auto health = service.health();
    trackengine::log::info("BOOT", "TrackEngine starting", {
        {"version", trackengine::EngineService::version()},
        {"schedule_db", schedule_db_path},
        {"ready", health.ready},
        {"host", host},
        {"port", port},
        {"threads", thread_count},
    });

    if (!server.listen(host, port)) {
        trackengine::log::error("BOOT", "Failed to bind", {
            {"host", host},
            {"port", port},
        });
        return 1;
    }
    return 0;
}
