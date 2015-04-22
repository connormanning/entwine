set(MODULE http)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/curl.cpp"
    "${BASE}/s3.cpp"
)

set(
    HEADERS
    "${BASE}/curl.hpp"
    "${BASE}/s3.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

