set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(ENTWINE_SRC ${SOURCES} "${BASE}/entwine.cpp")
set(MERGE_SRC ${SOURCES} "${BASE}/merge.cpp")

add_executable(entwine_kernel ${ENTWINE_SRC})
add_executable(merge_kernel ${MERGE_SRC})

add_dependencies(entwine_kernel entwine)
add_dependencies(merge_kernel entwine)

target_link_libraries(entwine_kernel entwine)
target_link_libraries(merge_kernel entwine)

set_target_properties(entwine_kernel PROPERTIES OUTPUT_NAME entwine)
set_target_properties(merge_kernel PROPERTIES OUTPUT_NAME merge)

install(TARGETS entwine_kernel DESTINATION bin)
install(TARGETS merge_kernel DESTINATION bin)

