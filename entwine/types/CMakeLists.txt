set(MODULE types)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/bounds.cpp"
    "${BASE}/file-info.cpp"
    "${BASE}/files.cpp"
    "${BASE}/metadata.cpp"
    "${BASE}/srs.cpp"
    "${BASE}/subset.cpp"
)

set(
    HEADERS
    "${BASE}/binary-point-table.hpp"
    "${BASE}/bounds.hpp"
    "${BASE}/dim-info.hpp"
    "${BASE}/dir.hpp"
    "${BASE}/file-info.hpp"
    "${BASE}/files.hpp"
    "${BASE}/fixed-point-layout.hpp"
    "${BASE}/key.hpp"
    "${BASE}/metadata.hpp"
    "${BASE}/point.hpp"
    "${BASE}/point-stats.hpp"
    "${BASE}/reprojection.hpp"
    "${BASE}/scale-offset.hpp"
    "${BASE}/schema.hpp"
    "${BASE}/srs.hpp"
    "${BASE}/subset.hpp"
    "${BASE}/vector-point-table.hpp"
    "${BASE}/version.hpp"
    "${BASE}/voxel.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})
