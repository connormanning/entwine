configure_file(unit/config.hpp.in "${CMAKE_CURRENT_BINARY_DIR}/unit/config.hpp")

macro(ENTWINE_ADD_TEST _name)
    set(test-name "${_name}-test")

    set(options)
    set(oneValueArgs)
    set(multiValueArgs FILES LINK_WITH)
    cmake_parse_arguments(ENTWINE_ADD_TEST
        "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_executable(${test-name} ${ENTWINE_ADD_TEST_FILES})
    compiler_options(${test-name})
    target_link_libraries(${test-name}
        PRIVATE entwine gtest gtest_main ${PDAL_LIBRARIES})
    target_include_directories(${test-name}
        PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/unit)
    target_compile_definitions(${test-name}
        PRIVATE GTEST_LINKED_AS_SHARED_LIBRARY=1)

    add_test(NAME ${_name} COMMAND ${test-name})
endmacro(ENTWINE_ADD_TEST)

ENTWINE_ADD_TEST(initialize FILES unit/init.cpp)
ENTWINE_ADD_TEST(version    FILES unit/version.cpp)
ENTWINE_ADD_TEST(srs        FILES unit/srs.cpp)
ENTWINE_ADD_TEST(scan       FILES unit/scan.cpp)
ENTWINE_ADD_TEST(build      FILES unit/build.cpp)
ENTWINE_ADD_TEST(read       FILES unit/read.cpp)

