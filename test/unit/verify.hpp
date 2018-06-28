#pragma once

#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/schema.hpp>

using namespace entwine;

class Verify
{
    using DimId = pdal::Dimension::Id;
    using DimType = pdal::Dimension::Type;

public:
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
            { DimId::X, DimType::Signed32 },
            { DimId::Y, DimType::Signed32 },
            { DimId::Z, DimType::Signed32 },
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

    Scale scale() const { return Scale(0.01); }
    uint64_t numPoints() const { return 100000; }
};

