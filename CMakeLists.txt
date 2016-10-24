cmake_minimum_required(VERSION 2.8.8)

project(entwine)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules" ${CMAKE_MODULE_PATH})

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR
    "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    add_definitions(${CMAKE_CXX_FLAGS} "-std=c++11")
    add_definitions(${CMAKE_CXX_FLAGS} "-Wno-deprecated-declarations")
    add_definitions(${CMAKE_CXX_FLAGS} "-Wall")
    add_definitions(${CMAKE_CXX_FLAGS} "-pedantic")
    add_definitions(${CMAKE_CXX_FLAGS} "-fexceptions")
    add_definitions(${CMAKE_CXX_FLAGS} "-fPIC")
endif()

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/json.cmake")

if (ENTWINE_JSONCPP_INCLUDE_DIR)
    MESSAGE("Using system JsonCpp")
    include_directories(${ENTWINE_JSONCPP_INCLUDE_DIR})
else()
    MESSAGE("Using bundled JsonCpp")
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/entwine/third")
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/entwine/third/json")
endif()

set(PDAL_FIND_VERSION 1.2)
find_package(PDAL ${PDAL_FIND_VERSION} REQUIRED CONFIG NO_POLICY_SCOPE)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads REQUIRED)
find_package(LazPerf REQUIRED)
find_package(Curl REQUIRED)

mark_as_advanced(CLEAR PDAL_INCLUDE_DIRS)
mark_as_advanced(CLEAR LazPerf_INCLUDE_DIR)
mark_as_advanced(CLEAR PDAL_LIBRARIES)
include_directories(${PDAL_INCLUDE_DIRS})
include_directories(${LAZPERF_INCLUDE_DIR})

if (CMAKE_MAJOR_VERSION GREATER 2)
    cmake_policy(SET CMP0022 OLD) # interface link libraries
    cmake_policy(SET CMP0042 NEW) # osx rpath
endif()

include_directories(BEFORE "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(entwine)
add_subdirectory(kernel)

set(OBJS
    $<TARGET_OBJECTS:formats>
    $<TARGET_OBJECTS:reader>
    $<TARGET_OBJECTS:third>
    $<TARGET_OBJECTS:tree>
    $<TARGET_OBJECTS:types>
    $<TARGET_OBJECTS:util>
)

add_library(entwine SHARED ${OBJS})

target_link_libraries(entwine pdalcpp lzma dl)

if (ENTWINE_JSONCPP_INCLUDE_DIR)
    MESSAGE("Linking system JsonCpp")
    target_link_libraries(entwine jsoncpp)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    target_link_libraries(entwine atomic)
endif()

if (WIN32)
    target_link_libraries(entwine)
else ()
    target_link_libraries(entwine ${CURL_LIBRARIES})
endif()

install(TARGETS entwine DESTINATION lib EXPORT entwine-targets)

export(EXPORT entwine-targets FILE "${PROJECT_BINARY_DIR}/entwine-targets.cmake")
configure_file(cmake/entwine-config.cmake.in "${PROJECT_BINARY_DIR}/entwine-config.cmake" @ONLY)
install(EXPORT entwine-targets FILE entwine-targets.cmake NAMESPACE Entwine:: DESTINATION lib/cmake/entwine)
install(FILES "${PROJECT_BINARY_DIR}/entwine-config.cmake" DESTINATION lib/cmake/entwine)

