set(MODULE json)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    ${SOURCES}
    "${BASE}/jsoncpp.cpp"
    PARENT_SCOPE
)

set(
    HEADERS
    ${HEADERS}
    "${BASE}/json.h"
    PARENT_SCOPE
)

install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

