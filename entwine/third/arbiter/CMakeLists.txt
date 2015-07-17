set(MODULE arbiter)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/arbiter.cpp"
)

set(
    HEADERS
    "${BASE}/arbiter.hpp"
)

set(THIRD_SOURCES ${THIRD_SOURCES} ${SOURCES} PARENT_SCOPE)
install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

