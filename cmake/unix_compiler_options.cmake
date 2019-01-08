function(system_compiler_options target)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        target_compile_options(${target}
            PRIVATE
                -Wno-deprecated-declarations
                -Wall
                -pedantic
                -fexceptions
        )
    else()
        target_compile_options(${target}
            PRIVATE
                -Wno-deprecated-declarations
                -Wall
                -pedantic
                -fexceptions
                -Werror
        )
    endif()
endfunction()
