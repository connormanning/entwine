/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>

namespace entwine
{

struct Subset
{
    Subset() = default;
    Subset(uint64_t id, uint64_t of);
    Subset(const json& j)
        : Subset(j.at("id").get<uint64_t>(), j.at("of").get<uint64_t>())
    { }
    uint64_t id = 0;
    uint64_t of = 0;
};

inline bool isPrimary(const Subset& s) { return s.id == 1; }

inline uint64_t getSplits(const Subset& s)
{
    return std::log2(s.of) / std::log2(4);
}

Bounds getBounds(Bounds cube, const Subset& s);

inline void to_json(json& j, const Subset& s)
{
    j = { { "id", s.id }, { "of", s.of } };
}

inline void from_json(const json& j, Subset& s) { s = Subset(j); }

} // namespace entwine
