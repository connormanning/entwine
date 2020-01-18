set(MODULE builder)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/chunk-cache.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/config.cpp"
    "${BASE}/hierarchy.cpp"
    # "${BASE}/merger.cpp"
    # "${BASE}/scan.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/chunk-cache.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/config.hpp"
    "${BASE}/heuristics.hpp"
    "${BASE}/hierarchy.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/overflow.hpp"
    "${BASE}/scan.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})
