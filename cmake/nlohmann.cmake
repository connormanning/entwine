#
# N Lohmann JSON handler
#

if(NOT CMAKE_REQUIRED_QUIET)
  # CMake 3.17+ use CHECK_START/CHECK_PASS/CHECK_FAIL
  message(STATUS "Looking for nlohmann")
endif()
find_package(nlohmann_json QUIET)
set(USE_EXTERNAL_NLOHMANN_DEFAULT OFF)
if(Nlohmann_FOUND)
  if(NOT CMAKE_REQUIRED_QUIET)
      message(STATUS "Looking for nlohmann - found (${Nlohmann_VERSION})")
  endif()
  if(Nlohmann_VERSION VERSION_LESS MIN_GTest_VERSION)
    message(WARNING "External GTest version is too old")
  else()
    set(USE_EXTERNAL_NLOHMANN_DEFAULT ON)
  endif()
else()
  if(NOT CMAKE_REQUIRED_QUIET)
    message(STATUS "Looking for nlohmann - not found")
  endif()
endif()

option(USE_EXTERNAL_NLOHMANN "Use an external nlohmann JSON library" OFF)

if(USE_EXTERNAL_NLOHMANN)
    find_package(nlohmann_json 3.11.3 REQUIRED)
else()

    include(FetchContent)

    FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
    FetchContent_MakeAvailable(json)

endif()



