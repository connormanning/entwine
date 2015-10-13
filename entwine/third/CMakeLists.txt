set(MODULE third)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/json")

add_subdirectory(arbiter)
add_subdirectory(bigint)
add_subdirectory(json)
add_subdirectory(splice-pool)

add_library(${MODULE} OBJECT ${THIRD_SOURCES})

