set(MODULE third)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(HEADERS "${BASE}/json.hpp")
install(FILES ${HEADERS} DESTINATION include/entwine/third)

add_subdirectory(arbiter)

add_library(${MODULE} OBJECT ${THIRD_SOURCES})
compiler_options(${MODULE})

