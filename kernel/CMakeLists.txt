set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(ENTWINE_SRC ${SOURCES} "${BASE}/entwine.cpp")
set(REPROJ_SRC  ${SOURCES} "${BASE}/post-reproject.cpp")

add_executable(entwine_kernel ${ENTWINE_SRC})
add_executable(reproj_kernel ${REPROJ_SRC})

target_link_libraries(entwine_kernel entwine pdalcpp curl crypto)
target_link_libraries(reproj_kernel  entwine pdalcpp pdal_util curl crypto)

set_target_properties(entwine_kernel PROPERTIES OUTPUT_NAME entwine)
set_target_properties(reproj_kernel  PROPERTIES OUTPUT_NAME reproj)

