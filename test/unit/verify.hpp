#pragma once

#include <entwine/types/bounds.hpp>
#include <entwine/types/schema.hpp>

using namespace entwine;

class Verify
{
public:
    Verify() { }

    Bounds bounds() const
    {
        return Bounds(-8242746, 4966506, -50, -8242446, 4966706, 50);
    }

    Bounds boundsUtm() const
    {
        return Bounds(580621, 4504618, -50, 580850, 4504771, 50);
    }

    Schema schema() const
    {
        return Schema(DimList {
            { DimId::X, DimType::Signed32, 0.01 },
            { DimId::Y, DimType::Signed32, 0.01 },
            { DimId::Z, DimType::Signed32, 0.01 },
            DimId::Intensity,
            DimId::ReturnNumber,
            DimId::NumberOfReturns,
            DimId::ScanDirectionFlag,
            DimId::EdgeOfFlightLine,
            DimId::Classification,
            DimId::ScanAngleRank,
            DimId::UserData,
            DimId::PointSourceId,
            DimId::GpsTime,
            DimId::Red,
            DimId::Green,
            DimId::Blue
        });
    }

    uint64_t points() const { return 100000; }
    uint64_t hierarchyStep() const { return 2; }
    uint64_t ticks() const { return 32; }
};

