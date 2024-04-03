#pragma once

#include <entwine/types/bounds.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/srs.hpp>

using namespace entwine;

struct Verify
{
    const Bounds bounds = Bounds(-8242746, 4966506, -50, -8242446, 4966706, 50);
    const Bounds boundsUtm = Bounds(
        580621.2087415651, 4504618.157951002, -50.0,
        580850.5686624339, 4504771.858998275, 50.0);
    const uint64_t points = 100000;
    const Srs srs = Srs("EPSG:3857");
    const Schema schema = Schema {
        {"X", Type::Signed32, 0.01},
        {"Y", Type::Signed32, 0.01},
        {"Z", Type::Signed32, 0.01},
        {"Intensity"},
        {"ReturnNumber"},
        {"NumberOfReturns"},
        {"ScanDirectionFlag"},
        {"EdgeOfFlightLine"},
        {"Classification"},
        {"Synthetic"},
        {"KeyPoint"},
        {"Withheld"},
        {"Overlap"},
        {"ScanAngleRank"},
        {"UserData"},
        {"PointSourceId"},
        {"GpsTime"},
        {"Red"},
        {"Green"},
        {"Blue"}
    };
};
