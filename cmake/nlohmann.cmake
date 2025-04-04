#
# N Lohmann JSON handler
#

option(USE_EXTERNAL_NLOHMANN "Use an external nlohmann JSON library" OFF)

if(USE_EXTERNAL_NLOHMANN)
    find_package(nlohmann_json 3.11.3 REQUIRED)
else()

    include(FetchContent)

    FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
    FetchContent_MakeAvailable(json)

endif()



