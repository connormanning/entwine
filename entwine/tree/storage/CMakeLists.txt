set(MODULE storage)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/laszip.cpp"
    "${BASE}/lazperf.cpp"
    "${BASE}/storage.cpp"
)

set(
    HEADERS
    "${BASE}/binary.hpp"
    "${BASE}/laszip.hpp"
    "${BASE}/lazperf.hpp"
    "${BASE}/storage.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/tree/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

