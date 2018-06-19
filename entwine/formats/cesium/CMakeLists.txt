set(MODULE formats)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/tileset.cpp"
)

set(
    HEADERS
    "${BASE}/tileset.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/formats/cesium)
add_library(${MODULE} OBJECT ${SOURCES})

