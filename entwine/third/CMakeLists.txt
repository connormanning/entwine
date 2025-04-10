set(MODULE third)
set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(arbiter)

add_library(${MODULE} OBJECT ${THIRD_SOURCES})
compiler_options(${MODULE})

