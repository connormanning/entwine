/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/threads.hpp>

#include <cmath>

#include <entwine/builder/heuristics.hpp>

namespace entwine
{

void from_json(const json& j, Threads& t)
{
    if (j.is_array())
    {
        t = Threads(j.at(0).get<uint64_t>(), j.at(1).get<uint64_t>());
        return;
    }

    const uint64_t total = j.is_number() ? j.get<uint64_t>() : 8;
    const uint64_t work =
        std::llround(total * heuristics::defaultWorkToClipRatio);
    assert(total >= work);
    const uint64_t clip = total - work;
    t = Threads(work, clip);
}

} // namespace entwine
