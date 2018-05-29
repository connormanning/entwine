set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/compression.cpp"
    "${BASE}/executor.cpp"
    "${BASE}/pool.cpp"
)

set(
    HEADERS
    "${BASE}/compression.hpp"
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

