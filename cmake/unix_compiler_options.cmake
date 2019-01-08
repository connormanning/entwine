function(system_compiler_options target)
    target_compile_options(${target}
        PRIVATE
            -Wno-deprecated-declarations
            -Wall
            -pedantic
            -fexceptions
    )
endfunction()

