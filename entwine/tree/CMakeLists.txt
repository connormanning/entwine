set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/self-chunk.cpp"
    "${BASE}/new-clipper.cpp"
    "${BASE}/config.cpp"
    "${BASE}/inference.cpp"
    "${BASE}/merger.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/sequence.cpp"
    "${BASE}/thread-pools.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/self-chunk.hpp"
    "${BASE}/new-climber.hpp"
    "${BASE}/new-clipper.hpp"
    "${BASE}/config.hpp"
    "${BASE}/heuristics.hpp"
    "${BASE}/hierarchy.hpp"
    "${BASE}/inference.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/sequence.hpp"
    "${BASE}/thread-pools.hpp"
    "${BASE}/traverser.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

