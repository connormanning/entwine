set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/compression.cpp"
    "${BASE}/executor.cpp"
    "${BASE}/lzma.cpp"
    "${BASE}/pool.cpp"
    "${BASE}/storage.cpp"
)

set(
    HEADERS
    "${BASE}/compression.hpp"
    "${BASE}/executor.hpp"
    "${BASE}/json.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/matrix.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/spin-lock.hpp"
    "${BASE}/storage.hpp"
    "${BASE}/stack-trace.hpp"
    "${BASE}/unique.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

