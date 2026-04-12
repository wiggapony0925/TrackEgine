#pragma once

// Structured JSON logger for TrackEngine.
// Outputs one JSON line per event to stdout — ideal for Render log drain.
//
// Usage:
//   trackengine::log::info("HTTP", "Server started", {{"port", 8080}});
//   trackengine::log::error("PLAN", "compute failed", {{"detail", e.what()}});
//   trackengine::log::request("POST", "/go", 200, 142.3, {{"trips", 4}});
//
// Timer helper for measuring durations:
//   trackengine::log::Timer t;
//   /* ... work ... */
//   double ms = t.elapsed_ms();

#include <chrono>
#include <string>

#include "nlohmann/json.hpp"

namespace trackengine {
namespace log {

enum class Level { DEBUG, INFO, WARN, ERROR };

/// Core emit function — writes one JSON line to stdout.
void emit(Level level, const std::string& tag, const std::string& msg,
          const nlohmann::json& extra = nullptr);

/// Convenience wrappers.
inline void debug(const std::string& tag, const std::string& msg,
                  const nlohmann::json& extra = nullptr) {
    emit(Level::DEBUG, tag, msg, extra);
}
inline void info(const std::string& tag, const std::string& msg,
                 const nlohmann::json& extra = nullptr) {
    emit(Level::INFO, tag, msg, extra);
}
inline void warn(const std::string& tag, const std::string& msg,
                 const nlohmann::json& extra = nullptr) {
    emit(Level::WARN, tag, msg, extra);
}
inline void error(const std::string& tag, const std::string& msg,
                  const nlohmann::json& extra = nullptr) {
    emit(Level::ERROR, tag, msg, extra);
}

/// Log an HTTP request with method, path, status, and timing.
void request(const std::string& method, const std::string& path,
             int status, double duration_ms,
             const nlohmann::json& extra = nullptr);

/// Minimal RAII timer.
struct Timer {
    using Clock = std::chrono::steady_clock;
    Clock::time_point start = Clock::now();

    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }
    double elapsed_s() const {
        return std::chrono::duration<double>(Clock::now() - start).count();
    }
    void reset() { start = Clock::now(); }
};

}  // namespace log
}  // namespace trackengine
