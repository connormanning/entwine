set(MODULE types)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/bounds.cpp"
    "${BASE}/dimension.cpp"
    "${BASE}/dimension-stats.cpp"
    "${BASE}/endpoints.cpp"
    "${BASE}/metadata.cpp"
    "${BASE}/source.cpp"
    "${BASE}/srs.cpp"
    "${BASE}/subset.cpp"
    "${BASE}/threads.cpp"
)

set(
    HEADERS
    "${BASE}/bounds.hpp"
    "${BASE}/build-parameters.hpp"
    "${BASE}/dim-info.hpp"
    "${BASE}/dimension.hpp"
    "${BASE}/dimension-stats.hpp"
    "${BASE}/dir.hpp"
    "${BASE}/endpoints.hpp"
    "${BASE}/exceptions.hpp"
    "${BASE}/fixed-point-layout.hpp"
    "${BASE}/key.hpp"
    "${BASE}/metadata.hpp"
    "${BASE}/point.hpp"
    "${BASE}/point-counts.hpp"
    "${BASE}/point-stats.hpp"
    "${BASE}/reprojection.hpp"
    "${BASE}/scale-offset.hpp"
    "${BASE}/source.hpp"
    "${BASE}/srs.hpp"
    "${BASE}/subset.hpp"
    "${BASE}/threads.hpp"
    "${BASE}/vector-point-table.hpp"
    "${BASE}/version.hpp"
    "${BASE}/voxel.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})
