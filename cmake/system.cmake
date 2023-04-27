if (WIN32)
    include(${CMAKE_DIR}/windows_compiler_options.cmake)
    find_library(SHLWAPI Shlwapi.lib)

else()
    include(${CMAKE_DIR}/unix_compiler_options.cmake)
endif()

function(compiler_options target)
    system_compiler_options(${target})
    set_property(TARGET ${target} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${target} PROPERTY CXX_STANDARD_REQUIRED TRUE)
    set_property(TARGET ${target} PROPERTY POSITION_INDEPENDENT_CODE TRUE)
    target_compile_definitions(${target}
        PRIVATE
            ${CURL_DEFS}
            ${OPENSSL_DEFS}
			${BACKTRACE_DEFS}
    )
    target_include_directories(${target}
        PRIVATE
			${ROOT_DIR}
            ${PROJECT_BINARY_DIR}/include
            ${PDAL_INCLUDE_DIRS}
            ${CURL_INCLUDE_DIR}
            ${OPENSSL_INCLUDE_DIR}
            ${LASZIP_DIRECTORIES}
			${JSONCPP_INCLUDE_DIR}
    )
endfunction()
