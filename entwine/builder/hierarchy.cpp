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
#include <memory>
#include <vector>

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
    Analysis(const Hierarchy::Map& sizes)
    {
        const double totalFiles = sizes.size();
        double totalNodes = 0;
        for (const auto& page : sizes)
        {
            const uint64_t chunkSize = page.second;
            totalNodes += chunkSize;
            maxNodesPerFile = std::max(maxNodesPerFile, chunkSize);
        }

        double mean = totalNodes / totalFiles;
        double ss = 0;
        for (const auto& page : sizes)
        {
            const double n = page.second;
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
    const Xyz& position = key.position();
    return Dxyz(
        key.d + 1,
        position.x * 2 + ((dir & 0x1) ? 1 : 0),
        position.y * 2 + ((dir & 0x2) ? 1 : 0),
        position.z * 2 + ((dir & 0x4) ? 1 : 0));
}

template <typename Page, typename Append, typename Complete>
void visitPages(
    const Hierarchy::Map& hierarchy,
    const Hierarchy::Map::value_type& node,
    const uint64_t pageDepth,
    const unsigned step,
    Page& page,
    const Append& append,
    const Complete& complete)
{
    const Dxyz& key = node.first;
    if (step && key.d > pageDepth && key.d % step == 0)
    {
        append(page, key, -1);
        Page child {};
        visitPages(hierarchy, node, key.d, step, child, append, complete);
        complete(key, std::move(child));
        return;
    }

    append(page, key, node.second);
    for (int dir = 0; dir < 8; ++dir)
    {
        const auto child = hierarchy.find(getChild(key, dir));
        if (child != hierarchy.end())
        {
            visitPages(hierarchy, *child, pageDepth, step, page, append, complete);
        }
    }
}

template <typename Page, typename Append, typename Complete>
void forEachPage(const Hierarchy& hierarchy, unsigned step, Append append, Complete complete)
{
    const auto root = hierarchy.map.find(Dxyz());
    if (root == hierarchy.map.end()) return;
    Page page {};
    visitPages(hierarchy.map, *root, root->first.d, step, page, append, complete);
    complete(root->first, std::move(page));
}

Hierarchy::Map countPages(const Hierarchy& hierarchy, unsigned step)
{
    Hierarchy::Map sizes;
    forEachPage<int64_t>(
        hierarchy, step,
        [](int64_t& count, const Dxyz&, int64_t) { ++count; },
        [&](const Dxyz& root, int64_t count) { sizes.emplace(root, count); });
    return sizes;
}

} // unnamed namespace

Hierarchy::ChunkMap getChunks(const Hierarchy& h, const unsigned step)
{
    Hierarchy::ChunkMap result;
    forEachPage<Hierarchy::Map>(
        h, step,
        [](Hierarchy::Map& page, const Dxyz& key, int64_t count) { page.emplace(key, count); },
        [&](const Dxyz& root, Hierarchy::Map&& page) { result.emplace(root, std::move(page)); });
    return result;
}

unsigned determineStep(const Hierarchy& h)
{
    if (h.map.size() < heuristics::maxHierarchyNodesPerFile) return 0;

    struct AnalysisEntry
    {
        AnalysisEntry(const Hierarchy& h, unsigned step)
            : analysis(countPages(h, step))
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

    struct PageEntry
    {
        const Dxyz* key;
        int64_t count;
    };
    using Page = std::vector<PageEntry>;

    forEachPage<Page>(
        h, step,
        [](Page& page, const Dxyz& key, int64_t count) { page.push_back({ &key, count }); },
        [&](const Dxyz& root, Page&& page)
        {
            const std::string filename = root.toString() + postfix + ".json";
            const int indent = root.d ? -1 : 2;
            // The pool copies tasks. Share the completed page so the queue
            // retains just its entries until a writer finishes with them.
            const auto counts = std::make_shared<const Page>(std::move(page));
            pool.add([&ep, filename, indent, counts]()
            {
                json data = json::object();
                for (const auto& node : *counts)
                {
                    data[node.key->toString()] = node.count;
                }
                ensurePut(ep, filename, data.dump(indent));
            });
        });

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
