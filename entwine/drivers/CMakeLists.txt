set(MODULE drivers)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/arbiter.cpp"
    "${BASE}/fs.cpp"
    "${BASE}/s3.cpp"
    "${BASE}/source.cpp"
)

set(
    HEADERS
    "${BASE}/arbiter.hpp"
    "${BASE}/driver.hpp"
    "${BASE}/fs.hpp"
    "${BASE}/s3.hpp"
    "${BASE}/source.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

