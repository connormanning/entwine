/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <algorithm>
#include <cstdint>

#include <entwine/util/json.hpp>

namespace entwine
{

struct Threads
{
    Threads() = default;
    Threads(uint64_t work, uint64_t clip)
        : work(std::max<uint64_t>(work, 1))
        , clip(std::max<uint64_t>(clip, 3))
    { }

    uint64_t work = 0;
    uint64_t clip = 0;
};

inline uint64_t getTotal(const Threads& t) { return t.work + t.clip; }
void from_json(const json& j, Threads& threads);

} // namespace entwine
