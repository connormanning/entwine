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
#include <entwine/util/json.hpp>

namespace entwine
{

Hierarchy::Hierarchy(
        const Metadata& m,
        const arbiter::Endpoint& top,
        const bool exists)
{
    if (exists)
    {
        const arbiter::Endpoint ep(top.getSubEndpoint("ept-hierarchy"));
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

        if (json[s].isBool()) load(m, ep, k);
        else m_map[k] = json[s].asUInt64();
    }
}

void Hierarchy::save(
        const Metadata& m,
        const arbiter::Endpoint& top,
        Pool& pool) const
{
    const arbiter::Endpoint ep(top.getSubEndpoint("ept-hierarchy"));

    Json::Value json(Json::objectValue);
    const ChunkKey k(m);
    save(m, ep, pool, k, json);

    const std::string f(filename(m, k));
    pool.add([&ep, f, json]() { ensurePut(ep, f, json.toStyledString()); });

    pool.await();
}

void Hierarchy::save(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        Pool& pool,
        const ChunkKey& k,
        Json::Value& curr) const
{
    const uint64_t n(get(k.dxyz()));
    if (!n) return;

    if (m_step && k.depth() && (k.depth() % m_step == 0))
    {
        curr[k.toString()] = true;

        Json::Value next;
        next[k.toString()] = static_cast<Json::UInt64>(n);

        for (uint64_t dir(0); dir < 8; ++dir)
        {
            save(m, ep, pool, k.getStep(toDir(dir)), next);
        }

        const std::string f(filename(m, k));
        pool.add([&ep, f, next]() { ensurePut(ep, f, toFastString(next)); });
    }
    else
    {
        curr[k.toString()] = static_cast<Json::UInt64>(n);

        for (uint64_t dir(0); dir < 8; ++dir)
        {
            save(m, ep, pool, k.getStep(toDir(dir)), curr);
        }
    }
}

void Hierarchy::analyze(const Metadata& m, const bool verbose) const
{
    if (m_step) return;
    if (m_map.size() <= heuristics::maxHierarchyNodesPerFile) return;

    AnalysisSet analysis;
    std::vector<uint64_t> steps{ 5, 6, 8, 10 };
    for (const uint64_t step : steps)
    {
        Map analyzed;
        const ChunkKey k(m);
        analyzed[k.dxyz()] = 1;
        analyze(m, step, k, k.dxyz(), analyzed);

        analysis.emplace(m_map, analyzed, step);
    }

    const auto& chosen(*analysis.begin());

    if (verbose)
    {
        for (const auto& a : analysis) a.summarize();
        std::cout << "Setting hierarchy step: " << chosen.step << std::endl;
    }

    m_step = chosen.step;
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

Hierarchy::Analysis::Analysis(
        const Hierarchy::Map& hierarchy,
        const Hierarchy::Map& analyzed,
        uint64_t step)
    : step(step)
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

void Hierarchy::Analysis::summarize() const
{
    std::cout <<
        "HS" << step <<
        " T: " << totalNodes <<
        " F: " << totalFiles <<
        " X: " << maxNodesPerFile <<
        " M: " << mean <<
        " D: " << stddev <<
        " R: " << rsd <<
        std::endl;
}

} // namespace entwine

