set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(ENTWINE_SRC ${SOURCES} "${BASE}/entwine.cpp")

add_executable(entwine_kernel ${ENTWINE_SRC})

add_dependencies(entwine_kernel entwine)
target_link_libraries(entwine_kernel entwine pdalcpp curl crypto)

set_target_properties(entwine_kernel PROPERTIES OUTPUT_NAME entwine)

install(TARGETS entwine_kernel DESTINATION bin)

