add_definitions(${CMAKE_CXX_FLAGS}
    "/DNOMINMAX" "/DJSON_DLL" "/DGTEST_LINKED_AS_SHARED_LIBRARY=1")

add_executable(entwine-test
    unit/main.cpp
    unit/srs.cpp
    unit/version.cpp
    unit/scan.cpp
    unit/build.cpp
    unit/read.cpp
)

configure_file(unit/config.hpp.in "${CMAKE_CURRENT_BINARY_DIR}/unit/config.hpp")
include_directories(${CMAKE_CURRENT_BINARY_DIR}/unit)

target_link_libraries(entwine-test entwine gtest gtest_main)

# We're overriding the test with a custom command for individual test output
# and colors, which cmake doesn't like.
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE INTERNAL "No dev warnings")

add_custom_target(
    test
    COMMAND entwine-test --gtest_color=yes
    DEPENDS entwine-test
    WORKING_DIRECTORY ${CMAKE_PROJECT_DIR}
)

