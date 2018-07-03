set(BASE "${CMAKE_CURRENT_SOURCE_DIR}")

set(
    SOURCES
    "${BASE}/build.cpp"
    "${BASE}/convert.cpp"
    "${BASE}/entwine.cpp"
    "${BASE}/merge.cpp"
    "${BASE}/scan.cpp"
)

add_executable(app ${SOURCES})
add_dependencies(app entwine)

target_link_libraries(app entwine ${Backtrace_LIBRARIES})
set_target_properties(app PROPERTIES OUTPUT_NAME entwine)

install(TARGETS app DESTINATION bin)

