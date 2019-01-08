set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/executor.cpp"
)

set(
    HEADERS
    "${BASE}/env.hpp"
    "${BASE}/executor.hpp"
    "${BASE}/json.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/matrix.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/spin-lock.hpp"
    "${BASE}/stack-trace.hpp"
    "${BASE}/time.hpp"
    "${BASE}/unique.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})
