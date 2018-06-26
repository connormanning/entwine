set(MODULE formats)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/tileset.cpp"
    "${BASE}/pnts.cpp"
)

set(
    HEADERS
    "${BASE}/tile.hpp"
    "${BASE}/tileset.hpp"
    "${BASE}/pnts.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/formats/cesium)
add_library(${MODULE} OBJECT ${SOURCES})

