set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/executor.cpp"
    "${BASE}/fs.cpp"
    "${BASE}/pool.cpp"
)

set(
    HEADERS
    "${BASE}/executor.hpp"
    "${BASE}/fs.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/object-pool.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/pooled.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

