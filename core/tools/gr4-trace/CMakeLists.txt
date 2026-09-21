# Built regardless of GR4_ENABLE_TRACING: reading a capture and producing one are unrelated, and an
# engineer analysing a trace is usually not the one who recorded it.
add_executable(gr4-trace gr4-trace.cpp)
target_link_libraries(gr4-trace PRIVATE gnuradio-options gnuradio-core)
