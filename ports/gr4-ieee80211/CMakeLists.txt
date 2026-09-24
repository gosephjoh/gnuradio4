# gr4-ieee80211 -- the GR4 port of the Phase 10 response-time graph.
# Consumes ~/gnuradio4 as a subdirectory; nothing of GR4's own tests,
# examples, benchmarks or block-registry plugins is built.
cmake_minimum_required(VERSION 3.27)
project(gr4ieee80211 CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# The GR4 tree: when this directory sits inside one (ports/gr4-ieee80211/ on
# the ieee80211-rx-latency branch of gosephjoh/gnuradio4) it is two levels
# up; otherwise ~/gnuradio4, or pass -DGNURADIO4_DIR=/path.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../../core/include/gnuradio-4.0/Block.hpp")
  get_filename_component(_gr4_default "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
else()
  set(_gr4_default "$ENV{HOME}/gnuradio4")
endif()
set(GNURADIO4_DIR "${_gr4_default}" CACHE PATH "gnuradio4 source tree")
message(STATUS "gr4-ieee80211: building against GNU Radio 4 at ${GNURADIO4_DIR}")
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ENABLE_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(GR_ENABLE_BLOCK_REGISTRY OFF CACHE BOOL "" FORCE)
set(INTERNAL_ENABLE_BLOCK_PLUGINS OFF CACHE BOOL "" FORCE)
set(WARNINGS_AS_ERRORS OFF CACHE BOOL "" FORCE)
set(USE_MOLD_LINKER OFF CACHE BOOL "" FORCE)
set(USE_CCACHE OFF CACHE BOOL "" FORCE)
set(GR_ENABLE_HTTP OFF CACHE STRING "" FORCE)
# The trace-marker layer (branch lightweight-tracing): compiled in by default
# so one binary serves traced and untraced runs -- with no category live a
# marker is one predicted branch (LIGHTWEIGHT_TRACING_REFERENCE 5.1).
option(GR4WIFI_TRACING "compile GR4's scheduler/work() trace markers in (inert until trace_categories is set)" ON)
set(GR4_ENABLE_TRACING ${GR4WIFI_TRACING} CACHE BOOL "" FORCE)
add_subdirectory(${GNURADIO4_DIR} gnuradio4 EXCLUDE_FROM_ALL)

# SIMD flags: GR3's -msse2 on x86; nothing on aarch64 (Raspberry Pi 5), where
# the Viterbi decoder falls back to upstream's portable implementation.
# GR4WIFI_GENERIC_VITERBI=ON forces that implementation on x86 too, which is
# how it is checked against the fixture (docs/batch-rt-experiments.md).
option(GR4WIFI_GENERIC_VITERBI "use the portable Viterbi decoder even where SSE2 exists" OFF)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64|i[3-6]86")
  set(GR4WIFI_SIMD_FLAGS -msse2)
else()
  set(GR4WIFI_SIMD_FLAGS "")
endif()

# nlohmann/json is vendored (third_party/, MIT) so the tree builds on a machine
# without the -dev package; a system copy is not needed.
add_library(gr4json INTERFACE)
target_include_directories(gr4json INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

# The receiver blocks: header-only, GR3's flags (-O3 -msse2, no -march=native)
add_library(gr4ieee80211 INTERFACE)
target_include_directories(gr4ieee80211 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(gr4ieee80211 INTERFACE
  gnuradio4::gnuradio-core gnuradio4::gnuradio-blocklib-core gnuradio4::gnuradio-algorithm
  gnuradio4::gr-basic gnuradio4::gr-fileio gnuradio4::gr-math gnuradio4::gr-testing)
target_compile_options(gr4ieee80211 INTERFACE -O3 ${GR4WIFI_SIMD_FLAGS})
if(GR4WIFI_GENERIC_VITERBI)
  target_compile_definitions(gr4ieee80211 INTERFACE GR4WIFI_GENERIC_VITERBI)
endif()

# One chain of the graph, instantiated once in its own translation unit
add_library(gr4chain STATIC src/chain.cpp)
target_link_libraries(gr4chain PUBLIC gr4ieee80211)

add_executable(rx_latency4 apps/rx_latency4.cpp)
target_link_libraries(rx_latency4 PRIVATE gr4chain gr4json)
# libgnuradio-core.a (PluginLoader) needs the registry symbols from the shared
# blocklib-core, which the linker would otherwise discard as unneeded
target_link_options(rx_latency4 PRIVATE -Wl,--no-as-needed)

add_executable(hello apps/hello.cpp)
target_link_libraries(hello PRIVATE gnuradio4::gnuradio-core gnuradio4::gnuradio-blocklib-core gnuradio4::gr-testing)

add_executable(probe apps/probe.cpp)
target_link_libraries(probe PRIVATE gr4chain)
target_link_options(probe PRIVATE -Wl,--no-as-needed)

# The standalone sample generator: no GNU Radio of either generation.
add_executable(gen4 apps/gen4.cpp)
target_include_directories(gen4 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(gen4 PRIVATE gr4json)
target_compile_options(gen4 PRIVATE -O2 ${GR4WIFI_SIMD_FLAGS})

# microbench4: the scheduler's per-invocation cost on a synthetic copy graph
add_executable(microbench4 apps/microbench4.cpp)
target_link_libraries(microbench4 PRIVATE gnuradio4::gnuradio-core gnuradio4::gnuradio-blocklib-core gnuradio4::gr-testing)
target_link_options(microbench4 PRIVATE -Wl,--no-as-needed)

# trace_export: a .gr4trace capture -> CSV tables for scripts/trace-batch-rt.py
add_executable(trace_export apps/trace_export.cpp)
target_link_libraries(trace_export PRIVATE gnuradio4::gnuradio-core gnuradio4::gnuradio-blocklib-core gr4json)
target_link_options(trace_export PRIVATE -Wl,--no-as-needed)

# The harness's own tests (Boost.UT, fetched by GR4's dependency helper, so off unless asked for:
# a machine without network access can still build everything above).
option(GR4WIFI_TESTING "build the harness's qa_ tests (fetches Boost.UT)" OFF)
if(GR4WIFI_TESTING)
  gr4_resolve_dependency(ut REQUIRED)
  enable_testing()
  add_executable(qa_Pacer tests/qa_Pacer.cpp)
  target_link_libraries(qa_Pacer PRIVATE gr4chain ut)
  target_link_options(qa_Pacer PRIVATE -Wl,--no-as-needed)
  add_test(NAME qa_Pacer COMMAND qa_Pacer)
  add_executable(qa_Chain tests/qa_Chain.cpp)
  target_link_libraries(qa_Chain PRIVATE gr4chain ut)
  target_link_options(qa_Chain PRIVATE -Wl,--no-as-needed)
  add_test(NAME qa_Chain COMMAND qa_Chain)
endif()
