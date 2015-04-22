set(MODULE tree)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/branch.cpp"
    "${BASE}/builder.cpp"
    "${BASE}/point-info.cpp"
    "${BASE}/reader.cpp"
    "${BASE}/registry.cpp"
    "${BASE}/roller.cpp"
)

set(
    HEADERS
    "${BASE}/branch.hpp"
    "${BASE}/builder.hpp"
    "${BASE}/point-info.hpp"
    "${BASE}/reader.hpp"
    "${BASE}/registry.hpp"
    "${BASE}/roller.hpp"
)

add_subdirectory(branches)

install(FILES ${HEADERS} DESTINATION include/entwine/${MODULE})
add_library(${MODULE} OBJECT ${SOURCES} ${BRANCHES_SOURCES})

