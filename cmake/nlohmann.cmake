#
# N Lohmann JSON handler
#

if(NOT CMAKE_REQUIRED_QUIET)
  # CMake 3.17+ use CHECK_START/CHECK_PASS/CHECK_FAIL
  message(STATUS "Looking for nlohmann")
endif()
find_package(nlohmann_json QUIET)
set(USE_EXTERNAL_NLOHMANN_DEFAULT OFF)
if(nlohmann_json_FOUND)
  if(NOT CMAKE_REQUIRED_QUIET)
      message(STATUS "Looking for nlohmann - found (${Nlohmann_VERSION})")
  endif()
  set(USE_EXTERNAL_NLOHMANN_DEFAULT ON)
else()
  if(NOT CMAKE_REQUIRED_QUIET)
    message(STATUS "Looking for nlohmann - not found")
  endif()
endif()

option(USE_EXTERNAL_NLOHMANN "Use an external nlohmann JSON library" ${USE_EXTERNAL_NLOHMANN_DEFAULT})

if(USE_EXTERNAL_NLOHMANN)
    find_package(nlohmann_json 3.11.3 REQUIRED)

    if(NOT nlohmann_json_FOUND)
        if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
            cmake_policy(SET CMP0135 NEW)
        endif()
        include(FetchContent)

        FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
        FetchContent_MakeAvailable(json)
    endif()

endif()



