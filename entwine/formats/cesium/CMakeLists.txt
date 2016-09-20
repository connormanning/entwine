set(MODULE formats)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/feature-table.cpp"
    "${BASE}/settings.cpp"
    "${BASE}/tile.cpp"
    "${BASE}/tile-builder.cpp"
    "${BASE}/tile-info.cpp"
)

set(
    HEADERS
    "${BASE}/feature-table.hpp"
    "${BASE}/settings.hpp"
    "${BASE}/tile.hpp"
    "${BASE}/tile-builder.hpp"
    "${BASE}/tile-info.hpp"
    "${BASE}/tileset.hpp"
    "${BASE}/util.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/formats/cesium)
add_library(${MODULE} OBJECT ${SOURCES})

