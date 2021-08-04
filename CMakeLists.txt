cmake_minimum_required(VERSION 2.8.12)

project(entwine)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules" ${CMAKE_MODULE_PATH})

set(ENTWINE_VERSION_MAJOR 2 CACHE STRING "Entwine major version" FORCE)
set(ENTWINE_VERSION_MINOR 2 CACHE STRING "Entwine minor version" FORCE)
set(ENTWINE_VERSION_PATCH 0 CACHE STRING "Entwine patch version" FORCE)
set(ENTWINE_VERSION_STRING
    "${ENTWINE_VERSION_MAJOR}.${ENTWINE_VERSION_MINOR}.${ENTWINE_VERSION_PATCH}"
    CACHE STRING "Entwine version string" FORCE)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(ENTWINE_API_VERSION "3")
set(ENTWINE_BUILD_VERSION ${ENTWINE_VERSION_STRING})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    MESSAGE("Defaulting to release mode")
    set(CMAKE_BUILD_TYPE "Release")
endif()

include(FeatureSummary)

set(ROOT_DIR ${PROJECT_SOURCE_DIR})
set(CMAKE_DIR ${ROOT_DIR}/cmake)
link_directories(${ADDITIONAL_LINK_DIRECTORIES_PATHS})

set(entwine_defs_hpp_in
    "${ROOT_DIR}/entwine/types/defs.hpp.in")
set(entwine_defs_hpp
    ${PROJECT_BINARY_DIR}/include/entwine/types/defs.hpp)
configure_file(${entwine_defs_hpp_in} ${entwine_defs_hpp})

option(WITH_ZSTD
    "Build support for compression/decompression with Zstd." TRUE)
if (NOT WITH_ZSTD)
    add_definitions(-DNO_ZSTD)
endif()

include(${CMAKE_DIR}/threads.cmake)
include(${CMAKE_DIR}/backtrace.cmake)
include(${CMAKE_DIR}/curl.cmake)
include(${CMAKE_DIR}/openssl.cmake)
include(${CMAKE_DIR}/pdal.cmake)
#
# Must come last.  Depends on vars set in other include files.
#
include(${CMAKE_DIR}/system.cmake)

add_subdirectory(entwine)
add_subdirectory(app)

#
# Each subdirectory is built as an object library, which is a collection
# of files which act as a target.  No actual file is built.
#
set(OBJS
    $<TARGET_OBJECTS:io>
    $<TARGET_OBJECTS:third>
    $<TARGET_OBJECTS:builder>
    $<TARGET_OBJECTS:types>
    $<TARGET_OBJECTS:util>
)

if (MSVC)
    add_library(entwine STATIC ${OBJS})
    target_link_libraries(entwine PRIVATE wsock32 ws2_32)
else()
    add_library(entwine SHARED ${OBJS})
endif()

target_link_libraries(entwine
    PUBLIC
        ${LZMA_LIBRARY}
        ${CMAKE_DL_LIBS}
    PRIVATE
        ${PDAL_LIBRARIES}
        ${CURL_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${SHLWAPI}
)

set_target_properties(
    entwine
    PROPERTIES
    VERSION ${ENTWINE_BUILD_VERSION}
    SOVERSION ${ENTWINE_API_VERSION}
    CLEAN_DIRECT_OUTPUT 1)

#
# Test
#
option(WITH_TESTS "Choose if Entwine unit tests should be built" TRUE)
if (WITH_TESTS)
    message("Configuring with unit tests")
    enable_testing()
    set(GOOGLETEST_VERSION 1.10.0)
    add_subdirectory("${PROJECT_SOURCE_DIR}/test/googletest-release-1.10.0/googletest")

    add_subdirectory(test)
else()
    message("Configuring with NO unit tests")
endif()

#
# Installation
#
install(TARGETS entwine DESTINATION lib EXPORT entwine-targets)
install(FILES ${entwine_defs_hpp} DESTINATION "include/entwine/types")
#export(
#    EXPORT entwine-targets
#    FILE "${PROJECT_BINARY_DIR}/entwine-targets.cmake")
configure_file(
    cmake/entwine-config.cmake.in "${PROJECT_BINARY_DIR}/entwine-config.cmake"
    @ONLY)
install(
    EXPORT entwine-targets
    FILE entwine-targets.cmake
    NAMESPACE Entwine:: DESTINATION lib/cmake/entwine)
install(
    FILES "${PROJECT_BINARY_DIR}/entwine-config.cmake"
    DESTINATION lib/cmake/entwine)

#
# Cpack
#
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cpack.cmake")
add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source)

feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES)
