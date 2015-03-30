set(MODULE branches)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/base.cpp"
    "${BASE}/flat.cpp"
    "${BASE}/disk.cpp"
    "${BASE}/clipper.cpp"
)

set(
    HEADERS
    "${BASE}/base.hpp"
    "${BASE}/flat.hpp"
    "${BASE}/disk.hpp"
    "${BASE}/clipper.hpp"
)

set(BRANCHES_SOURCES ${SOURCES} PARENT_SCOPE)
install(FILES ${HEADERS} DESTINATION include/entwine/tree/${MODULE})

