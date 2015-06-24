cmake_minimum_required(VERSION 2.8.8)

project(entwine)

add_definitions(${CMAKE_CXX_FLAGS} "-std=c++11")
add_definitions(${CMAKE_CXX_FLAGS} "-O3")
add_definitions(${CMAKE_CXX_FLAGS} "-Wno-deprecated-declarations")
add_definitions(${CMAKE_CXX_FLAGS} "-Wall")
add_definitions(${CMAKE_CXX_FLAGS} "-Werror")
add_definitions(${CMAKE_CXX_FLAGS} "-pedantic")
add_definitions(${CMAKE_CXX_FLAGS} "-pthread")
add_definitions(${CMAKE_CXX_FLAGS} "-fexceptions")
add_definitions(${CMAKE_CXX_FLAGS} "-fPIC")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/entwine/third")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/entwine/third/json")


set(PDAL_BASE "" CACHE STRING "PDAL installation base directory")

if (PDAL_BASE)
    message("PDAL_BASE overridden: " ${PDAL_BASE})
    include_directories("${PDAL_BASE}/include")
    link_directories("${PDAL_BASE}/lib")
endif()

add_subdirectory(entwine)
add_subdirectory(kernel)

set(OBJS
    $<TARGET_OBJECTS:compression>
    $<TARGET_OBJECTS:drivers>
    $<TARGET_OBJECTS:reader>
    $<TARGET_OBJECTS:third>
    $<TARGET_OBJECTS:tree>
    $<TARGET_OBJECTS:types>
    $<TARGET_OBJECTS:util>
)

add_library(entwine SHARED ${OBJS})
target_link_libraries(entwine pdalcpp curl crypto pthread)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    target_link_libraries(entwine atomic)
endif()

install(TARGETS entwine DESTINATION lib)

