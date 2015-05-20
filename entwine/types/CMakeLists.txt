set(MODULE types)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/bbox.cpp"
    "${BASE}/dim-info.cpp"
    "${BASE}/linking-point-view.cpp"
    "${BASE}/reprojection.cpp"
    "${BASE}/schema.cpp"
    "${BASE}/simple-point-layout.cpp"
    "${BASE}/simple-point-table.cpp"
    "${BASE}/single-point-table.cpp"
    "${BASE}/structure.cpp"
)

set(
    HEADERS
    "${BASE}/bbox.hpp"
    "${BASE}/dim-info.hpp"
    "${BASE}/elastic-atomic.hpp"
    "${BASE}/linking-point-view.hpp"
    "${BASE}/point.hpp"
    "${BASE}/reprojection.hpp"
    "${BASE}/schema.hpp"
    "${BASE}/simple-point-layout.hpp"
    "${BASE}/simple-point-table.hpp"
    "${BASE}/single-point-table.hpp"
    "${BASE}/sized-point-table.hpp"
    "${BASE}/structure.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

