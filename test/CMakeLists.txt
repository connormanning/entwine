add_executable(entwine-test
    unit/main.cpp
    unit/srs.cpp
    unit/version.cpp
    unit/scan.cpp
    unit/build.cpp
    unit/read.cpp
)
compiler_options(entwine-test)

configure_file(unit/config.hpp.in "${CMAKE_CURRENT_BINARY_DIR}/unit/config.hpp")

target_link_libraries(entwine-test
    PRIVATE
        entwine
        gtest
        gtest_main
        ${PDAL_LIBRARIES}
        ${JSONCPP_LIBRARY}
)
target_include_directories(entwine-test
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}/unit
)
target_compile_definitions(entwine-test
    PRIVATE
        GTEST_LINKED_AS_SHARED_LIBRARY=1
)

# We're overriding the test with a custom command for individual test output
# and colors, which cmake doesn't like.
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE INTERNAL "No dev warnings")

add_custom_target(
    test
    COMMAND entwine-test --gtest_color=yes
    DEPENDS entwine-test
    WORKING_DIRECTORY ${ROOT_DIR}
)

