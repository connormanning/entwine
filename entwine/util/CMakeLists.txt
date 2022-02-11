set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/config.cpp"
    "${BASE}/fs.cpp"
    "${BASE}/info.cpp"
    "${BASE}/io.cpp"
    "${BASE}/pipeline.cpp"
)

set(
    HEADERS
    "${BASE}/config.hpp"
    "${BASE}/env.hpp"
    "${BASE}/fs.hpp"
    "${BASE}/info.hpp"
    "${BASE}/io.hpp"
    "${BASE}/json.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/matrix.hpp"
    "${BASE}/optional.hpp"
    "${BASE}/pdal-mutex.hpp"
    "${BASE}/pipeline.hpp"
    "${BASE}/pool.hpp"
    "${BASE}/spin-lock.hpp"
    "${BASE}/stack-trace.hpp"
    "${BASE}/time.hpp"
    "${BASE}/unique.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})
compiler_options(${MODULE})

if(PDAL_HAS_LAS_REFACTOR)
    # PDAL LAS API changes between 2.3.0 & 2.4 https://github.com/PDAL/PDAL/commit/dd00e3a7
    add_compile_definitions(PDAL_HAS_LAS_REFACTOR)
endif()
