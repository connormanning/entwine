set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/generate-ellipsoid.cpp"
)

add_executable(generate-test-data ${SOURCES})
add_dependencies(generate-test-data entwine)

target_link_libraries(generate-test-data entwine)
set_target_properties(generate-test-data PROPERTIES OUTPUT_NAME generate)