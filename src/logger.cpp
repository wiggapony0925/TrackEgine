#include "trackengine/logger.hpp"

#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

namespace trackengine {
namespace log {

namespace {

// Single mutex so log lines never interleave across threads.
std::mutex g_log_mutex;

const char* level_string(Level level) {
    switch (level) {
        case Level::DEBUG: return "DEBUG";
        case Level::INFO:  return "INFO";
        case Level::WARN:  return "WARN";
        case Level::ERROR: return "ERROR";
    }
    return "INFO";
}

// ISO-8601 timestamp with milliseconds — matches backend log format.
std::string iso_timestamp() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto time = system_clock::to_time_t(now);
    const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &time);
#else
    gmtime_r(&time, &utc);
#endif

    std::ostringstream ss;
    ss << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
       << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
    return ss.str();
}

}  // namespace

void emit(Level level, const std::string& tag, const std::string& msg,
          const nlohmann::json& extra) {
    nlohmann::json line;
    line["ts"] = iso_timestamp();
    line["level"] = level_string(level);
    line["tag"] = tag;
    line["msg"] = msg;

    // Merge extra fields into the top-level object.
    if (!extra.is_null() && extra.is_object()) {
        for (auto it = extra.begin(); it != extra.end(); ++it) {
            line[it.key()] = it.value();
        }
    }

    const std::string output = line.dump() + '\n';
    {
        std::lock_guard<std::mutex> lock(g_log_mutex);
        std::cout << output;
        std::cout.flush();
    }
}

void request(const std::string& method, const std::string& path,
             int status, double duration_ms,
             const nlohmann::json& extra) {
    // Build "POST /go → 200 (42.3ms)" style message.
    std::ostringstream msg;
    msg << method << " " << path << " → " << status
        << " (" << std::fixed << std::setprecision(1) << duration_ms << "ms)";

    Level level = (status >= 500) ? Level::ERROR
                : (status >= 400) ? Level::WARN
                                  : Level::INFO;

    nlohmann::json merged;
    merged["method"] = method;
    merged["path"] = path;
    merged["status"] = status;
    merged["duration_ms"] = std::round(duration_ms * 10.0) / 10.0;
    if (!extra.is_null() && extra.is_object()) {
        for (auto it = extra.begin(); it != extra.end(); ++it) {
            merged[it.key()] = it.value();
        }
    }
    emit(level, "HTTP", msg.str(), merged);
}

}  // namespace log
}  // namespace trackengine
