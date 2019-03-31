set(MODULE builder)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/new-chunk.cpp"
    "${BASE}/chunk-cache.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/config.cpp"
    "${BASE}/hierarchy.cpp"
    "${BASE}/merger.cpp"
    "${BASE}/pruner.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/scan.cpp"
    "${BASE}/sequence.cpp"
    "${BASE}/thread-pools.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/new-chunk.hpp"
    "${BASE}/chunk-cache.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/config.hpp"
    "${BASE}/heuristics.hpp"
    "${BASE}/hierarchy.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/overflow.hpp"
    "${BASE}/pruner.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/scan.hpp"
    "${BASE}/sequence.hpp"
    "${BASE}/thread-pools.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})
