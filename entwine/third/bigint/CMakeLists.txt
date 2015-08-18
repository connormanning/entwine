set(MODULE bigint)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/little-big-int.cpp"
)

set(
    HEADERS
    "${BASE}/little-big-int.hpp"
)

set(THIRD_SOURCES ${THIRD_SOURCES} ${SOURCES} PARENT_SCOPE)
install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

