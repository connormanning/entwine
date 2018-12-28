set(MODULE reader)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/query.cpp"
    "${BASE}/reader.cpp"
    "${BASE}/chunk-reader.cpp"
    "${BASE}/cache.cpp"
    "${BASE}/comparison.cpp"
    "${BASE}/logic-gate.cpp"
)

set(
    HEADERS
    "${BASE}/reader.hpp"
    "${BASE}/cache.hpp"
    "${BASE}/chunk-reader.hpp"
    "${BASE}/hierarchy-reader.hpp"
    "${BASE}/query-params.hpp"
    "${BASE}/query.hpp"
    "${BASE}/comparison.hpp"
    "${BASE}/filter.hpp"
    "${BASE}/filterable.hpp"
    "${BASE}/logic-gate.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES} ${BRANCHES_SOURCES})
compiler_options(${MODULE})
