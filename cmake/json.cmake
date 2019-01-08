find_package(JsonCpp 1.6.2 REQUIRED)

if (JSONCPP_FOUND)
    mark_as_advanced(CLEAR JSONCPP_INCLUDE_DIR)
    mark_as_advanced(CLEAR JSONCPP_LIBRARY)
    set(ENTWINE_HAVE_JSONCPP 1)
endif()

