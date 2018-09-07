cmake_minimum_required(VERSION 2.8.8)

project(entwine)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules" ${CMAKE_MODULE_PATH})

set(ENTWINE_VERSION_MAJOR 2 CACHE STRING "Entwine major version" FORCE)
set(ENTWINE_VERSION_MINOR 0 CACHE STRING "Entwine minor version" FORCE)
set(ENTWINE_VERSION_PATCH 0 CACHE STRING "Entwine patch version" FORCE)
set(ENTWINE_VERSION_STRING
    "${ENTWINE_VERSION_MAJOR}.${ENTWINE_VERSION_MINOR}.${ENTWINE_VERSION_PATCH}"
    CACHE STRING "Entwine version string" FORCE)

set(ENTWINE_API_VERSION "2")
set(ENTWINE_BUILD_VERSION ${ENTWINE_VERSION_STRING})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    MESSAGE("Defaulting to release mode")
    set(CMAKE_BUILD_TYPE "Release")
endif()

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU" OR
    ${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
    add_definitions(${CMAKE_CXX_FLAGS} "-std=c++11")
    add_definitions(${CMAKE_CXX_FLAGS} "-Wno-deprecated-declarations")
    add_definitions(${CMAKE_CXX_FLAGS} "-Wall")
    add_definitions(${CMAKE_CXX_FLAGS} "-pedantic")
    add_definitions(${CMAKE_CXX_FLAGS} "-fexceptions")
    add_definitions(${CMAKE_CXX_FLAGS} "-fPIC")
endif()

if (MSVC)
    # prevents clashes between macros min\max and std::min\std::max
    add_definitions(${CMAKE_CXX_FLAGS} "/DNOMINMAX" "/DJSON_DLL")
endif(MSVC)

set(entwine_defs_hpp_in
    "${CMAKE_CURRENT_SOURCE_DIR}/entwine/types/defs.hpp.in")
set(entwine_defs_hpp
    "${CMAKE_CURRENT_BINARY_DIR}/include/entwine/types/defs.hpp")

configure_file(${entwine_defs_hpp_in} ${entwine_defs_hpp})
install(FILES ${entwine_defs_hpp} DESTINATION "include/entwine/types")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/include")

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

if (WIN32)
    find_library(WS2_32_LIBRARY ws2_32)
    find_library(SHLWAPI Shlwapi.lib)
else()
    find_package(Backtrace)
    if (${Backtrace_FOUND})
        message("Found backtrace ${Backtrace_LIBRARIES}")
        add_definitions("-DENTWINE_HAVE_BACKTRACE")
    else()
        message("Backtrace was not found")
    endif()
endif()

find_package(Curl)
if (CURL_FOUND)
    message("Found curl")
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    find_package(Threads REQUIRED)
    include_directories(${CURL_INCLUDE_DIRS})
    set(ENTWINE_CURL TRUE)
    add_definitions("-DENTWINE_CURL")
    add_definitions("-DARBITER_CURL")
else()
    message("Curl NOT found")
endif()

find_package(OpenSSL 1.1)
if (OPENSSL_FOUND)
    message("Found OpenSSL ${OPENSSL_VERSION}")
    include_directories(${OPENSSL_INCLUDE_DIR})
    set(ARBITER_OPENSSL TRUE)
    set(ENTWINE_OPENSSL TRUE)
    add_definitions("-DARBITER_OPENSSL")
    add_definitions("-DENTWINE_OPENSSL")
else()
    # For me this is /usr/local/opt/openssl\@1.1
    message("OpenSSL NOT found - `export OPENSSL_ROOT_DIR=___`")
    message("Google storage IO will not be available")
endif()


get_target_property(PDALCPP_INCLUDE_DIRS pdalcpp INTERFACE_INCLUDE_DIRECTORIES)
if (PDALCPP_INCLUDE_DIRS)
    message("Including from PDAL: ${PDALCPP_INCLUDE_DIRS}")
    include_directories(${PDALCPP_INCLUDE_DIRS})
else()
    include_directories(/usr/include/laszip)
    include_directories(/usr/local/include/laszip)
endif()

mark_as_advanced(CLEAR PDAL_INCLUDE_DIRS)
mark_as_advanced(CLEAR PDAL_LIBRARIES)
include_directories(${PDAL_INCLUDE_DIRS})

if (CMAKE_MAJOR_VERSION GREATER 2)
    cmake_policy(SET CMP0042 NEW) # osx rpath
endif()

include_directories(BEFORE "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(entwine)
add_subdirectory(app)

set(OBJS
    # $<TARGET_OBJECTS:formats>
    # $<TARGET_OBJECTS:reader>
    $<TARGET_OBJECTS:io>
    $<TARGET_OBJECTS:third>
    $<TARGET_OBJECTS:builder>
    $<TARGET_OBJECTS:types>
    $<TARGET_OBJECTS:util>
)

if (MSVC)
    add_library(entwine STATIC ${OBJS})
    target_link_libraries(entwine wsock32 ws2_32)
else()
    add_library(entwine SHARED ${OBJS})
endif()

target_link_libraries(entwine PUBLIC pdalcpp pdal_util ${LZMA_LIBRARY} ${CMAKE_DL_LIBS})

target_link_libraries(entwine ${JSON_CPP_LINK_TYPE} ${ENTWINE_JSONCPP_LIB_NAME})

if (WIN32)
    target_link_libraries(entwine PRIVATE ${SHLWAPI})
endif()

target_link_libraries(entwine PRIVATE ${CURL_LIBRARIES})
target_include_directories(entwine PRIVATE "${CURL_INCLUDE_DIR}")

target_link_libraries(entwine PRIVATE ${OPENSSL_LIBRARIES})
target_include_directories(entwine PRIVATE "${OPENSSL_INCLUDE_DIR}")

set_target_properties(
    entwine
    PROPERTIES
    VERSION ${ENTWINE_BUILD_VERSION}
    SOVERSION ${ENTWINE_API_VERSION}
    CLEAN_DIRECT_OUTPUT 1)

install(TARGETS entwine DESTINATION lib EXPORT entwine-targets)

export(
    EXPORT entwine-targets
    FILE "${PROJECT_BINARY_DIR}/entwine-targets.cmake")

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

# add_subdirectory(test/gtest-1.8.0)
# include_directories(entwine test/gtest-1.8.0/include test/gtest-1.8.0)
# add_subdirectory(test)

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cpack.cmake")
add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source)

