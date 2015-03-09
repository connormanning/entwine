set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/multi-batcher.cpp"
    "${BASE}/point-info.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/roller.cpp"
    "${BASE}/sleepy-tree.cpp"
    "${BASE}/branches/base.cpp"
)

set(
    HEADERS
    "${BASE}/branch.hpp"
    "${BASE}/multi-batcher.hpp"
    "${BASE}/point-info.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/roller.hpp"
    "${BASE}/sleepy-tree.hpp"
    "${BASE}/branches/base.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES})

