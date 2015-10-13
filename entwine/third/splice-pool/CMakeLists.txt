set(MODULE splice-pool)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    HEADERS
    "${BASE}/splice-pool.hpp"
)

install(FILES ${HEADERS} DESTINATION include/entwine/third/${MODULE})

