find_package(PDAL 1.8 REQUIRED CONFIG NO_POLICY_SCOPE)

get_target_property(PDALCPP_INCLUDE_DIRS pdalcpp INTERFACE_INCLUDE_DIRECTORIES)
if (PDALCPP_INCLUDE_DIRS)
    message("Including from PDAL: ${PDALCPP_INCLUDE_DIRS}")
	set(LASZIP_DIRECTORIES ${PDALCPP_INCLUDE_DIRS})
else()
	set(LASZIP_DIRECTORIES /usr/include/laszip /usr/local/include/laszip)
endif()
