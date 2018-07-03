find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
find_library(ZSTD_LIBRARY NAMES zstd)

find_package_handle_standard_args(ZSTD ZSTD_INCLUDE_DIR ZSTD_LIBRARY)

if (ZSTD_FOUND)
    set(ZSTD_LIBRARIES ${ZSTD_LIBRARY})
    set(ZSTD_INCLUDE_DIRS ${ZSTD_INCLUDE_DIR})
endif()

