set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/executor.cpp"
    "${BASE}/inference.cpp"
    "${BASE}/pool.cpp"
    "${BASE}/storage.cpp"
)

set(
    HEADERS
    "${BASE}/executor.hpp"
    "${BASE}/inference.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/storage.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

