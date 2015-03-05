set(MODULE types)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/bbox.cpp"
    "${BASE}/dim-info.cpp"
    "${BASE}/schema.cpp"
)

set(
    HEADERS
    "${BASE}/bbox.hpp"
    "${BASE}/dim-info.hpp"
    "${BASE}/elastic-atomic.hpp"
    "${BASE}/point.hpp"
    "${BASE}/schema.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

