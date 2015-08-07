set(MODULE pool)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    HEADERS
    "${BASE}/pool.hpp"
    "${BASE}/pool.cpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

