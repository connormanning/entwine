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

#include <algorithm>
#include <cassert>
#include <cmath>

#include <entwine/builder/heuristics.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

void to_json(json& j, const Hierarchy& h)
{
    j = json::object();
    for (const auto& entry : h.map) j[entry.first.toString()] = entry.second;
}

void from_json(const json& j, Hierarchy& h)
{
    h.map = j.get<Hierarchy::Map>();
    /*
    for (const auto& p : j.items())
    {
        const Dxyz key(p.key());
        const uint64_t value = p.value().get<uint64_t>();
        h.map[key] = value;
    }
    */
}

namespace hierarchy
{

namespace
{

struct Analysis
{
    Analysis() = default;
    Analysis(const Hierarchy::ChunkMap& chunks)
    {
        const double totalFiles = chunks.size();
        double totalNodes = 0;
        for (const auto& chunk : chunks)
        {
            const uint64_t chunkSize = chunk.second.size();
            totalNodes += chunkSize;
            maxNodesPerFile = std::max(maxNodesPerFile, chunkSize);
        }

        double mean = totalNodes / totalFiles;
        double ss = 0;
        for (const auto& chunk : chunks)
        {
            const double n = chunk.second.size();
            ss += std::pow(n - mean, 2.0);
        }
        double stddev = std::sqrt(ss / (totalNodes - 1.0));
        rsd = stddev / mean;
    }

    uint64_t maxNodesPerFile = 0;
    double rsd = 0;
};

Dxyz getChild(const Dxyz& key, const int dir)
{
    return Dxyz(
        key.d + 1,
        key.x * 2 + ((dir & 0x1) ? 1 : 0),
        key.y * 2 + ((dir & 0x2) ? 1 : 0),
        key.z * 2 + ((dir & 0x4) ? 1 : 0));
}

void getChunks(
    Hierarchy::ChunkMap& result,
    const Dxyz& root,
    const Dxyz& curr,
    const Hierarchy::Map& h,
    const unsigned step)
{
    if (!h.count(curr)) return;
    const int64_t n = h.at(curr);

    if (step && curr.d > root.d && curr.d % step == 0)
    {
        // Start a new subtree.
        result[root][curr] = -1;
        getChunks(result, curr, curr, h, step);
    }
    else
    {
        result[root][curr] = n;
        for (int dir = 0; dir < 8; ++dir)
        {
            getChunks(result, root, getChild(curr, dir), h, step);
        }
    }
}

} // unnamed namespace

Hierarchy::ChunkMap getChunks(const Hierarchy& h, const unsigned step)
{
    Hierarchy::ChunkMap result;
    getChunks(result, Dxyz(), Dxyz(), h.map, step);
    return result;
}

unsigned determineStep(const Hierarchy& h)
{
    if (h.map.size() < heuristics::maxHierarchyNodesPerFile) return 0;

    struct AnalysisEntry
    {
        AnalysisEntry(const Hierarchy& h, unsigned step)
            : analysis(getChunks(h, step))
            , step(step)
        { }
        Analysis analysis;
        unsigned step = 0;
    };

    std::vector<AnalysisEntry> entries;
    for (const uint64_t step : { 4, 5, 6, 8, 10 })
    {
        entries.emplace_back(h, step);
    }

    const auto best = std::min_element(
        entries.begin(),
        entries.end(),
        [](const AnalysisEntry& a, const AnalysisEntry& b)
        {
            const auto max = heuristics::maxHierarchyNodesPerFile;
            const bool afits = a.analysis.maxNodesPerFile < max;
            const bool bfits = b.analysis.maxNodesPerFile < max;

            if (afits && !bfits) return true;
            if (bfits && !afits) return false;

            if (a.analysis.rsd < b.analysis.rsd / 5.0) return true;
            if (b.analysis.rsd < a.analysis.rsd / 5.0) return false;

            // Prefer the higher step if their RSDs are close enough.
            return a.step > b.step;
        });

    return best->step;
}

void save(
    const Hierarchy& h,
    const arbiter::Endpoint& ep,
    const unsigned step,
    const unsigned threads,
    const std::string postfix)
{
    Pool pool(threads);

    const auto chunks = getChunks(h, step);
    for (const auto& chunk : chunks)
    {
        pool.add([&]()
        {
            const Dxyz& root = chunk.first;
            const Hierarchy::Map& counts = chunk.second;

            const std::string filename = root.toString() + postfix + ".json";
            json data = json::object();
            for (const auto& node : counts)
            {
                data[node.first.toString()] = node.second;
            }

            const int indent = root.d ? -1 : 2;
            ensurePut(ep, filename, data.dump(indent));
        });
    }

    pool.join();
}

void load(
    Hierarchy& h,
    const arbiter::Endpoint& ep,
    Pool& pool,
    const std::string& postfix,
    const Dxyz& root = Dxyz())
{
    const json j = json::parse(
        ensureGet(ep, root.toString() + postfix + ".json"));

    for (const auto& node : j.items())
    {
        const Dxyz key(node.key());
        const int64_t val(node.value().get<int64_t>());

        if (val == -1)
        {
            pool.add([&h, &ep, &pool, &postfix, key]()
            {
                load(h, ep, pool, postfix, key);
            });
        }
        else set(h, key, val);
    }
}

Hierarchy load(
    const arbiter::Endpoint& ep,
    const unsigned threads,
    const std::string postfix)
{
    Hierarchy hierarchy;
    Pool pool(threads);

    load(hierarchy, ep, pool, postfix);

    pool.await();

    return hierarchy;
}

} // namespace hierarchy
} // namespace entwine
