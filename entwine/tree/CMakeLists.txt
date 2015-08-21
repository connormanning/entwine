set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/climber.cpp"
    "${BASE}/cold.cpp"
    "${BASE}/config-parser.cpp"
    "${BASE}/manifest.cpp"
    "${BASE}/point-info.cpp"
    "${BASE}/registry.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/climber.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/cold.hpp"
    "${BASE}/config-parser.hpp"
    "${BASE}/manifest.hpp"
    "${BASE}/point-info.hpp"
    "${BASE}/registry.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

