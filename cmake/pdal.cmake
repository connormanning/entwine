include(CheckCXXSymbolExists)

find_package(PDAL 2.1.0 REQUIRED CONFIG NO_POLICY_SCOPE)

get_target_property(PDALCPP_INCLUDE_DIRS pdalcpp INTERFACE_INCLUDE_DIRECTORIES)
if (PDALCPP_INCLUDE_DIRS)
    message("Including from PDAL: ${PDALCPP_INCLUDE_DIRS}")
	set(LASZIP_DIRECTORIES ${PDALCPP_INCLUDE_DIRS})
else()
	set(LASZIP_DIRECTORIES /usr/include/laszip /usr/local/include/laszip)
endif()

# PDAL LAS API changes between 2.3.0 & 2.4 https://github.com/PDAL/PDAL/commit/dd00e3a7
get_target_property(CMAKE_REQUIRED_INCLUDES pdalcpp INTERFACE_INCLUDE_DIRECTORIES)
set(CMAKE_REQUIRED_LIBRARIES pdalcpp)
set(CMAKE_REQUIRED_FLAGS "-std=c++${CMAKE_CXX_STANDARD}")

check_cxx_symbol_exists("pdal::las::baseCount" "pdal/io/LasHeader.hpp" PDAL_HAS_LAS_REFACTOR)

unset(CMAKE_REQUIRED_FLAGS)
unset(CMAKE_REQUIRED_LIBRARIES)
unset(CMAKE_REQUIRED_INCLUDES)
