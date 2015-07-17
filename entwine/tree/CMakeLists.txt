set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/builder.cpp"
    "${BASE}/cold.cpp"
    "${BASE}/chunk.cpp"
    "${BASE}/clipper.cpp"
    "${BASE}/manifest.cpp"
    "${BASE}/point-info.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/roller.cpp"
)

set(
    HEADERS
    "${BASE}/builder.hpp"
    "${BASE}/cold.hpp"
    "${BASE}/chunk.hpp"
    "${BASE}/clipper.hpp"
    "${BASE}/manifest.hpp"
    "${BASE}/point-info.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/roller.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

