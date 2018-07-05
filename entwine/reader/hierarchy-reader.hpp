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

    HierarchyReader(const Metadata& metadata, const arbiter::Endpoint& out)
        : m_metadata(metadata)
        , m_ep(out.getSubEndpoint("h"))
        , m_step(m_metadata.hierarchyStep())
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

        for (const auto s : json.getMemberNames())
        {
            const Dxyz key(s);
            assert(key == root || resident(key) == root);
            m_keys[key] = json[s].asUInt64();

            if (
                    m_step &&
                    key.depth() > root.depth() &&
                    key.depth() % m_step == 0)
            {
                load(key);
            }
        }
    }

    Dxyz resident(const Dxyz& node)
    {
        if (!m_step || node.depth() <= m_step) return Dxyz();

        // Subtract 1 since nodes which are at the stepped depths are duplicated
        // in their parent resident, so use the upper one.
        const uint64_t residentDepth((node.depth() - 1) / m_step * m_step);
        const uint64_t offset(node.depth() - residentDepth);
        return Dxyz(
                residentDepth,
                node.x >> offset,
                node.y >> offset,
                node.z >> offset);
    }

    const Metadata& m_metadata;
    const arbiter::Endpoint m_ep;
    const uint64_t m_step;

    Keys m_keys;
};

} // namespace entwine

