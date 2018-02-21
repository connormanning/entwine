set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/new-chunk.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/new-clipper.cpp"
    "${BASE}/cold.cpp"
    "${BASE}/config-parser.cpp"
    "${BASE}/config.cpp"
    "${BASE}/inference.cpp"
    "${BASE}/merger.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/sequence.cpp"
    "${BASE}/thread-pools.cpp"
    "${BASE}/tiler.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/new-chunk.hpp"
    "${BASE}/climber.hpp"
    "${BASE}/new-climber.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/new-clipper.hpp"
    "${BASE}/cold.hpp"
    "${BASE}/config-parser.hpp"
    "${BASE}/config.hpp"
    "${BASE}/heuristics.hpp"
    "${BASE}/inference.hpp"
    "${BASE}/key.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/sequence.hpp"
    "${BASE}/splitter.hpp"
    "${BASE}/thread-pools.hpp"
    "${BASE}/tiler.hpp"
    "${BASE}/traverser.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

