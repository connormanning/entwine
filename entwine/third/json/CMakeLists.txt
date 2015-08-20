set(MODULE json)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/jsoncpp.cpp"
)

set(
    HEADERS
    "${BASE}/json.hpp"
)

set(THIRD_SOURCES ${THIRD_SOURCES} ${SOURCES} PARENT_SCOPE)
install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

