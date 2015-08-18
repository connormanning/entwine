set(MODULE pool)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    HEADERS
    "${BASE}/memory-pool.hpp"
    "${BASE}/memory-pool.cpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

