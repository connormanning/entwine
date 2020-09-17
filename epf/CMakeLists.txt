cmake_minimum_required(VERSION 3.5)
project(EPF VERSION 1.0 LANGUAGES CXX)

find_package(Pdal REQUIRED)

file(GLOB SRCS *.cpp)

add_executable(epf ${SRCS})

target_include_directories(epf PRIVATE ${PDAL_INCLUDE_DIRS})
target_link_libraries(epf PRIVATE ${PDAL_LIBRARIES})

set_property(TARGET epf PROPERTY CXX_STANDARD 11)
set_property(TARGET epf PROPERTY CXX_STANDARD_REQUIRED TRUE)
