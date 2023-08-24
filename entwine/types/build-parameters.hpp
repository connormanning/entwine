/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstdint>

#include <entwine/builder/heuristics.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

struct BuildParameters
{
    BuildParameters() = default;
    BuildParameters(
        uint64_t minNodeSize,
        uint64_t maxNodeSize,
        uint64_t cacheSize,
        uint64_t sleepCount,
        uint64_t progressInterval,
        uint64_t hierarchyStep,
        bool verbose = true,
        bool laz_14 = false,
        bool withSchemaStats = true)
        : minNodeSize(minNodeSize)
        , maxNodeSize(maxNodeSize)
        , cacheSize(cacheSize)
        , sleepCount(sleepCount)
        , progressInterval(progressInterval)
        , hierarchyStep(hierarchyStep)
        , verbose(verbose)
        , laz_14(laz_14)
        , withSchemaStats(withSchemaStats)
    { }
    BuildParameters(uint64_t minNodeSize, uint64_t maxNodeSize)
        : minNodeSize(minNodeSize)
        , maxNodeSize(maxNodeSize)
    { }

    uint64_t minNodeSize = 0;
    uint64_t maxNodeSize = 0;

    uint64_t cacheSize = heuristics::cacheSize;
    uint64_t sleepCount = heuristics::sleepCount;
    uint64_t progressInterval = 10;
    uint64_t hierarchyStep = 0;
    bool verbose = true;
    bool laz_14 = false;
    bool withSchemaStats = true;
};

inline void to_json(json& j, const BuildParameters& p)
{
    j = {
        { "software", "Entwine" },
        { "version", currentEntwineVersion() },
        { "minNodeSize", p.minNodeSize },
        { "maxNodeSize", p.maxNodeSize },
        { "laz_14", p.laz_14 },
        { "withSchemaStats", p.withSchemaStats }
    };
    if (p.hierarchyStep) j.update({ { "hierarchyStep", p.hierarchyStep } });
}

} // namespace entwine
