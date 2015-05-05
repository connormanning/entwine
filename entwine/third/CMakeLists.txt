set(MODULE third)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(json)

add_library(${MODULE} OBJECT ${THIRD_SOURCES})

