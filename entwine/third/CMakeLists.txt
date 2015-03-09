set(MODULE third)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

include_directories("${BASE}")

set(
    SOURCES
    "${BASE}/jsoncpp.cpp"
)

set(
    HEADERS
    "${BASE}/json/json.h"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

