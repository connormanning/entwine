set(MODULE third)

add_subdirectory(arbiter)

if (NOT ENTWINE_HAVE_JSONCPP)
    MESSAGE("Including bundled JsonCpp")
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}")
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/json")
    add_subdirectory(json)
else()
    MESSAGE("Skipping bundled JsonCpp")
endif()

add_library(${MODULE} OBJECT ${THIRD_SOURCES})
compiler_options(${MODULE})
