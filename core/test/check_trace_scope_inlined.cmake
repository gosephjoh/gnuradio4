# A trace scope whose category is off must cost a load and a branch at the call site, which holds only
# while its constructor is inlined there. Inlining is decided per call site, so a timing test on a small
# loop cannot see a regression -- its loop is inlined either way. The symbol table can: an out-of-line
# constructor is called by every scope, and is passed its record through memory.
#
#     cmake -DNM=<nm> -DBINARY=<test binary> -P check_trace_scope_inlined.cmake

execute_process(
  COMMAND ${NM} -C ${BINARY}
  OUTPUT_VARIABLE symbols
  RESULT_VARIABLE status)
if(NOT status EQUAL 0)
  message(FATAL_ERROR "${NM} could not read ${BINARY}")
endif()

# The positive control: the clock path is out of line on purpose, so its absence means the binary holds
# no scopes or the names did not demangle, and the check below would pass for the wrong reason.
string(FIND "${symbols}" "gr::trace::Scope::firstInstant()" control)
if(control EQUAL -1)
  message(FATAL_ERROR "no gr::trace::Scope::firstInstant() in ${BINARY}: nothing to check against")
endif()

string(FIND "${symbols}" "gr::trace::Scope::Scope(gr::trace::Event)" constructor)
if(NOT constructor EQUAL -1)
  message(FATAL_ERROR "gr::trace::Scope's constructor was compiled out of line in ${BINARY}")
endif()
