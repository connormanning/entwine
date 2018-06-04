/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/hierarchy.hpp>

#include <entwine/io/ensure.hpp>
#include <entwine/types/metadata.hpp>

namespace entwine
{

Hierarchy::Hierarchy(
        const Metadata& m,
        const arbiter::Endpoint& top,
        const bool exists)
{
    if (exists)
    {
        const arbiter::Endpoint ep(top.getSubEndpoint("h"));
        load(m, ep);
    }
}

void Hierarchy::load(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const Dxyz& root)
{
    const auto json(parse(ep.get(filename(m, root))));

    for (const auto s : json.getMemberNames())
    {
        const Dxyz k(s);
        assert(!m_map.count(k) || m_map.at(k) == json[s].asUInt64());

        m_map[k] = json[s].asUInt64();

        if (
                (m.hierarchyStep()) &&
                (k.depth() > root.depth()) &&
                (k.depth() % m.hierarchyStep() == 0))
        {
            load(m, ep, k);
        }
    }
}

void Hierarchy::save(const Metadata& m, const arbiter::Endpoint& top) const
{
    const arbiter::Endpoint ep(top.getSubEndpoint("h"));

    Json::Value json;
    const ChunkKey k(m);
    save(m, ep, k, json);

    ensurePut(ep, filename(m, k), json.toStyledString());
}

void Hierarchy::save(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const ChunkKey& k,
        Json::Value& curr) const
{
    const uint64_t n(get(k.dxyz()));
    if (!n) return;

    curr[k.toString()] = static_cast<Json::UInt64>(n);

    if (m.hierarchyStep() && k.depth() && (k.depth() % m.hierarchyStep() == 0))
    {
        Json::Value next;
        next[k.toString()] = static_cast<Json::UInt64>(n);

        for (uint64_t dir(0); dir < 8; ++dir)
        {
            save(m, ep, k.getStep(toDir(dir)), next);
        }

        ensurePut(ep, filename(m, k), next.toStyledString());
    }
    else
    {
        for (uint64_t dir(0); dir < 8; ++dir)
        {
            save(m, ep, k.getStep(toDir(dir)), curr);
        }
    }
}

} // namespace entwine

