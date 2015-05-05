set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/fs.cpp"
    "${BASE}/pool.cpp"
    "${BASE}/platform.cpp"
)

set(
    HEADERS
    "${BASE}/fs.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/platform.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

