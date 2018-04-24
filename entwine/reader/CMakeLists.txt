set(MODULE reader)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/cache.cpp"
    "${BASE}/chunk-reader.cpp"
    "${BASE}/comparison.cpp"
    "${BASE}/hierarchy-reader.cpp"
    "${BASE}/logic-gate.cpp"
    "${BASE}/query.cpp"
    "${BASE}/reader.cpp"
    "${BASE}/new-reader.cpp"
)

set(
    HEADERS
    "${BASE}/append.hpp"
    "${BASE}/cache.hpp"
    "${BASE}/chunk-reader.hpp"
    "${BASE}/comparison.hpp"
    "${BASE}/filter.hpp"
    "${BASE}/filterable.hpp"
    "${BASE}/hierarchy-reader.hpp"
    "${BASE}/logic-gate.hpp"
    "${BASE}/query.hpp"
    "${BASE}/query-chunk-state.hpp"
    "${BASE}/query-params.hpp"
    "${BASE}/reader.hpp"
    "${BASE}/new-reader.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES} ${BRANCHES_SOURCES})

