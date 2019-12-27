set(MODULE util)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/executor.cpp"
    "${BASE}/fs.cpp"
    "${BASE}/info.cpp"
    "${BASE}/io.cpp"
    "${BASE}/pipeline.cpp"
)

set(
    HEADERS
    "${BASE}/env.hpp"
    "${BASE}/executor.hpp"
    "${BASE}/fs.hpp"
    "${BASE}/info.hpp"
    "${BASE}/io.hpp"
    "${BASE}/json.hpp"
    "${BASE}/locker.hpp"
    "${BASE}/matrix.hpp"
    "${BASE}/optional.hpp"
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
