set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/build.cpp"
    "${BASE}/entwine.cpp"
    "${BASE}/merge.cpp"
)

add_executable(kernel ${SOURCES})
add_dependencies(kernel entwine)

target_link_libraries(kernel entwine)
set_target_properties(kernel PROPERTIES OUTPUT_NAME entwine)

install(TARGETS kernel DESTINATION bin)

