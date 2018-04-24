set(MODULE new-reader)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/new-query.cpp"
    "${BASE}/new-reader.cpp"
    "${BASE}/new-chunk-reader.cpp"
    "${BASE}/new-cache.cpp"
    "${BASE}/comparison.cpp"
    "${BASE}/logic-gate.cpp"
)

set(
    HEADERS
    "${BASE}/new-reader.hpp"
    "${BASE}/new-cache.hpp"
    "${BASE}/new-chunk-reader.hpp"
    "${BASE}/hierarchy-reader.hpp"
    "${BASE}/query-params.hpp"
    "${BASE}/new-query.hpp"
    "${BASE}/comparison.hpp"
    "${BASE}/filter.hpp"
    "${BASE}/filterable.hpp"
    "${BASE}/logic-gate.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES} ${BRANCHES_SOURCES})

