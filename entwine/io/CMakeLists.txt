set(MODULE io)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/binary.cpp"
    "${BASE}/ensure.cpp"
    "${BASE}/io.cpp"
    "${BASE}/laszip.cpp"
    "${BASE}/zstandard.cpp"
)

set(
    HEADERS
    "${BASE}/binary.hpp"
    "${BASE}/ensure.hpp"
    "${BASE}/io.hpp"
    "${BASE}/laszip.hpp"
    "${BASE}/zstandard.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/types/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

