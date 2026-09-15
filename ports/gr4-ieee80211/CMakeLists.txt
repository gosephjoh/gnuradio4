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
add_subdirectory(${GNURADIO4_DIR} gnuradio4 EXCLUDE_FROM_ALL)

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
target_compile_options(gr4ieee80211 INTERFACE -O3 -msse2)

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
target_compile_options(gen4 PRIVATE -O2 -msse2)
