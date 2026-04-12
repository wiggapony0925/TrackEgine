cmake_minimum_required(VERSION 3.20)

project(
    TrackEngine
    VERSION 0.2.0
    DESCRIPTION "Standalone C++ transit routing service for Track"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 REQUIRED)

add_executable(trackengine
    src/main.cpp
    src/engine_service.cpp
    src/logger.cpp
)

target_include_directories(trackengine
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party
)

target_link_libraries(trackengine PRIVATE SQLite::SQLite3)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(trackengine PRIVATE -Wall -Wextra -Wpedantic -O3)
endif()
