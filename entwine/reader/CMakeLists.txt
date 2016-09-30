set(MODULE reader)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/cache.cpp"
    "${BASE}/chunk-reader.cpp"
    "${BASE}/comparison.cpp"
    "${BASE}/logic-gate.cpp"
    "${BASE}/query.cpp"
    "${BASE}/reader.cpp"
)

set(
    HEADERS
    "${BASE}/cache.hpp"
    "${BASE}/chunk-reader.hpp"
    "${BASE}/comparison.hpp"
    "${BASE}/filter.hpp"
    "${BASE}/filterable.hpp"
    "${BASE}/logic-gate.hpp"
    "${BASE}/query.hpp"
    "${BASE}/reader.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES} ${BRANCHES_SOURCES})

