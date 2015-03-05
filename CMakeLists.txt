cmake_minimum_required(VERSION 2.8.6)

project(entwine)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -Wno-deprecated-declarations -Wall -Werror -pedantic -fexceptions -frtti -g"
)

set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

include_directories(
    "${BASE}"
    "${BASE}/third/"
)

add_subdirectory(compression)
add_subdirectory(http)
add_subdirectory(kernel)
add_subdirectory(third)
add_subdirectory(tree)
add_subdirectory(types)

add_library(
    entwine
    SHARED
    $<TARGET_OBJECTS:compression>
    $<TARGET_OBJECTS:http>
    $<TARGET_OBJECTS:third>
    $<TARGET_OBJECTS:tree>
    $<TARGET_OBJECTS:types>
)

target_link_libraries(entwine pdalcpp curl crypto)
install(TARGETS entwine DESTINATION lib)

