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

#include <cstdint>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class HierarchyReader
{
public:
    using Keys = std::map<Dxyz, uint64_t>;

    HierarchyReader(const arbiter::Endpoint& ep)
        : m_keys(([&ep]()
        {
            Keys keys;
            const Json::Value json(parse(ep.get("entwine-hierarchy.json")));
            for (const auto k : json.getMemberNames())
            {
                keys[Dxyz(k)] = json[k].asUInt64();
            }
            return keys;
        })())
    { }

    uint64_t count(const Dxyz& p) const
    {
        const auto it(m_keys.find(p));
        if (it != m_keys.end()) return it->second;
        else return 0;
    }

private:
    const Keys m_keys;
};

} // namespace entwine

