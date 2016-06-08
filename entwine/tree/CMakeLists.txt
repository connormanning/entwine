set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/climber.cpp"
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/cold.cpp"
    "${BASE}/config-parser.cpp"
    "${BASE}/hierarchy.cpp"
    "${BASE}/manifest.cpp"
    "${BASE}/merger.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/thread-pools.cpp"
    # "${BASE}/tiler.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/climber.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/cold.hpp"
    "${BASE}/config-parser.hpp"
    "${BASE}/hierarchy.hpp"
    "${BASE}/manifest.hpp"
    "${BASE}/merger.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/thread-pools.hpp"
    # "${BASE}/tiler.hpp"
    "${BASE}/traverser.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

