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
        PRIVATE entwine
            GTest::gtest
            GTest::gtest_main
            OpenSSL::applink
            OpenSSL::Crypto
            pdalcpp
        )
    if (CURL_FOUND)
        target_link_libraries(${test-name}
            PRIVATE
                CURL::libcurl
        )
    endif()

    target_include_directories(${test-name}
        PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/unit)
    target_compile_definitions(${test-name}
        PRIVATE GTEST_LINKED_AS_SHARED_LIBRARY=1)

    add_test(NAME ${_name} COMMAND ${test-name})

    if (WIN32)
        set_property(TEST ${_name} PROPERTY
            ENVIRONMENT_MODIFICATION
            PATH=path_list_append:$<TARGET_RUNTIME_DLL_DIRS:gtest_main>)
    endif()

endmacro(ENTWINE_ADD_TEST)

ENTWINE_ADD_TEST(initialize FILES unit/init.cpp)

ENTWINE_ADD_TEST(info FILES unit/info.cpp)
ENTWINE_ADD_TEST(build FILES unit/build.cpp)
ENTWINE_ADD_TEST(pipeline FILES unit/pipeline-utils.cpp)
ENTWINE_ADD_TEST(srs FILES unit/srs.cpp)
ENTWINE_ADD_TEST(time FILES unit/time.cpp)
ENTWINE_ADD_TEST(version FILES unit/version.cpp)
