set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/file-descriptor.cpp"
    "${BASE}/fs.cpp"
    "${BASE}/point-mapper.cpp"
    "${BASE}/platform.cpp"
)

set(
    HEADERS
    "${BASE}/file-descriptor.hpp"
    "${BASE}/fs.hpp"
    "${BASE}/point-mapper.hpp"
    "${BASE}/platform.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

