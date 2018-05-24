set(MODULE builder)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/config.cpp"
    "${BASE}/merger.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/scan.cpp"
    "${BASE}/sequence.cpp"
    "${BASE}/thread-pools.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/config.hpp"
    "${BASE}/heuristics.hpp"
    "${BASE}/hierarchy.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/scan.hpp"
    "${BASE}/sequence.hpp"
    "${BASE}/thread-pools.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

