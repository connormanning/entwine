set(MODULE chunk-storage)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/chunk-storage.cpp"
    "${BASE}/laszip.cpp"
    "${BASE}/lazperf.cpp"
)

set(
    HEADERS
    "${BASE}/binary.hpp"
    "${BASE}/chunk-storage.hpp"
    "${BASE}/laszip.hpp"
    "${BASE}/lazperf.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/types/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

