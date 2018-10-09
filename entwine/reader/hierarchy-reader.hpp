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

#include <cassert>
#include <cstdint>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Node
{
public:
};

class HierarchyReader
{
public:
    using Keys = std::map<Dxyz, uint64_t>;

    HierarchyReader(const arbiter::Endpoint& out)
        : m_ep(out.getSubEndpoint("ept-hierarchy"))
    {
        load();
    }

    uint64_t count(const Dxyz& p) const
    {
        const auto it(m_keys.find(p));
        if (it != m_keys.end()) return it->second;
        else return 0;
    }

private:
    // For now, we'll just load everything on init.  This needs to be hooked up
    // to a caching mechanism.
    void load(const Dxyz& root = Dxyz())
    {
        const auto json(parse(m_ep.get(root.toString() + ".json")));

        for (const auto str : json.getMemberNames())
        {
            const Dxyz key(str);
            const int64_t n(json[str].asInt64());
            if (n < 0) load(key);
            else m_keys[key] = static_cast<uint64_t>(n);
        }
    }

    const arbiter::Endpoint m_ep;
    Keys m_keys;
};

} // namespace entwine

