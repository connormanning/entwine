set(MODULE compression)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/stream.cpp"
    "${BASE}/util.cpp"
)

set(
    HEADERS
    "${BASE}/stream.hpp"
    "${BASE}/util.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

