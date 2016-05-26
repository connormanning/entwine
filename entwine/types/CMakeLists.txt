set(MODULE types)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/bbox.cpp"
    "${BASE}/blocked-data.cpp"
    "${BASE}/dim-info.cpp"
    "${BASE}/pooled-point-table.cpp"
    "${BASE}/schema.cpp"
    "${BASE}/simple-point-layout.cpp"
    "${BASE}/structure.cpp"
    "${BASE}/subset.cpp"
    "${BASE}/tube.cpp"
)

set(
    HEADERS
    "${BASE}/bbox.hpp"
    "${BASE}/blocked-data.hpp"
    "${BASE}/dim-info.hpp"
    "${BASE}/dir.hpp"
    "${BASE}/elastic-atomic.hpp"
    "${BASE}/outer-scope.hpp"
    "${BASE}/point.hpp"
    "${BASE}/point-pool.hpp"
    "${BASE}/pooled-point-table.hpp"
    "${BASE}/range.hpp"
    "${BASE}/reprojection.hpp"
    "${BASE}/schema.hpp"
    "${BASE}/simple-point-layout.hpp"
    "${BASE}/structure.hpp"
    "${BASE}/subset.hpp"
    "${BASE}/tube.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

