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

Hierarchy::Analysis::Analysis(
        const Hierarchy::Map& hierarchy,
        const Hierarchy::Map& analyzed,
        uint64_t step)
    : step(step)
    , idealNodes(hierarchy.size())
    , totalFiles(analyzed.size())
{
    for (const auto& p : analyzed)
    {
        const uint64_t n(p.second);
        totalNodes += n;
        maxNodesPerFile = std::max(maxNodesPerFile, n);
    }

    mean = (double)totalNodes / analyzed.size();

    double ss(0);
    for (const auto& p : analyzed)
    {
        const double n(p.second);
        ss += std::pow(n - mean, 2.0);
    }

    stddev = std::sqrt(ss / ((double)totalNodes - 1.0));
    rsd = stddev / mean;
}

Hierarchy::AnalysisSet Hierarchy::analyze(const Metadata& m) const
{
    AnalysisSet result;
    std::vector<uint64_t> steps{ 5, 6, 8, 10 };
    for (const uint64_t step : steps)
    {
        Map analyzed;
        const ChunkKey k(m);
        analyzed[k.dxyz()] = 1;
        analyze(m, step, k, k.dxyz(), analyzed);

        result.emplace(m_map, analyzed, step);
    }

    return result;
}

void Hierarchy::Analysis::summarize() const
{
    std::cout <<
        "HS" << step <<
        " I: " << idealNodes <<
        " T: " << totalNodes <<
        " F: " << totalFiles <<
        " S: " << maxNodesPerFile <<
        " M: " << mean <<
        " D: " << stddev <<
        " R: " << rsd <<
        std::endl;
}

bool Hierarchy::Analysis::operator<(const Hierarchy::Analysis& b) const
{
    const Hierarchy::Analysis& a(*this);

    if (a.fits() && !b.fits()) return true;
    if (b.fits() && !a.fits()) return false;

    if (a.rsd < b.rsd / 5.0) return true;
    if (b.rsd < a.rsd / 5.0) return false;

    // Prefer the higher step if their RSDs are close enough.
    return a.step > b.step;
}

void Hierarchy::analyze(
    const Metadata& m,
    const uint64_t step,
    const ChunkKey& k,
    const Dxyz& curr,
    Hierarchy::Map& map) const
{
    const uint64_t n(get(k.dxyz()));
    if (!n) return;

    ++map[curr];

    if (step && k.depth() && (k.depth() % step == 0))
    {
        const Dxyz next(k.dxyz());
        map[next] = 1;

        for (uint64_t dir(0); dir < 8; ++dir)
        {
            analyze(m, step, k.getStep(toDir(dir)), next, map);
        }
    }
    else
    {
        for (uint64_t dir(0); dir < 8; ++dir)
        {
            analyze(m, step, k.getStep(toDir(dir)), curr, map);
        }
    }
}

} // namespace entwine

