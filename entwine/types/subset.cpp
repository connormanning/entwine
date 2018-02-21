/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

/*
#include <entwine/types/subset.hpp>

#include <set>

#include <entwine/tree/climber.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

Subset::Subset(
        const Bounds& bounds,
        const std::size_t id,
        const std::size_t of)
    : m_id(id - 1)
    , m_of(of)
    , m_sub()
    , m_minimumNullDepth(1)
{
    if (!id) throw std::runtime_error("Subset IDs should be 1-based.");
    if (id > of) throw std::runtime_error("Invalid subset ID - too large.");

    if (m_of <= 1)
    {
        throw std::runtime_error("Invalid subset range");
    }

    // Always split only in X-Y, since data tends not to be dense throughout
    // the entire Z-range.
    const std::size_t dimensions(2);
    const std::size_t factor(4);

    const std::size_t log(std::log2(m_of));

    if (static_cast<std::size_t>(std::pow(2, log)) != m_of)
    {
        throw std::runtime_error("Subset range must be a power of 2");
    }

    std::size_t cap(factor);

    while (cap < m_of)
    {
        ++m_minimumNullDepth;
        cap *= factor;
    }

    const std::size_t boxes(cap / m_of);
    const std::size_t startOffset(m_id * boxes);

    const std::size_t iterations(ChunkInfo::logN(cap, factor));
    const std::size_t mask(0x3);

    bool set(false);

    for (std::size_t curId(startOffset); curId < startOffset + boxes; ++curId)
    {
        Bounds current(bounds);

        for (std::size_t i(iterations - 1); i < iterations; --i)
        {
            const Dir dir(toDir(curId >> (i * dimensions) & mask));
            current.go(dir, true);
        }

        m_boxes.push_back(current);

        if (!set)
        {
            m_sub = current;
            set = true;
        }
        else
        {
            m_sub.grow(current);
        }
    }
}

Subset::Subset(const Bounds& bounds, const Json::Value& json)
    : Subset(bounds, json["id"].asUInt64(), json["of"].asUInt64())
{ }

Json::Value Subset::toJson() const
{
    Json::Value json;

    json["id"] = static_cast<Json::UInt64>(m_id + 1);
    json["of"] = static_cast<Json::UInt64>(m_of);

    return json;
}

std::size_t Subset::minimumBaseDepth(const std::size_t pointsPerChunk) const
{
    const std::size_t nominalChunkDepth(ChunkInfo::logN(pointsPerChunk, 4));
    std::size_t min(nominalChunkDepth);

    std::size_t chunksAtDepth(1);

    while (chunksAtDepth < m_of)
    {
        ++min;
        chunksAtDepth *= 4;
    }

    return min;
}

std::vector<Subset::Span> Subset::calcSpans(
        const Structure& structure,
        const Bounds& bounds) const
{
    if (structure.maxChunksPerDepth() < m_of)
    {
        throw std::runtime_error(
                "Maximum chunks per depth < number of subsets");
    }

    const std::size_t depthEnd(structure.baseDepthEnd());
    std::set<Subset::Span> spans;

    for (const auto& b : m_boxes)
    {
        ChunkState c(structure, bounds);
        while (c.chunkBounds() != b && c.chunkBounds().contains(b))
        {
            if (c.depth() > structure.sparseDepthBegin())
            {
                throw std::runtime_error("Exceeded sparse depth");
            }

            c.climb(b.mid());
        }

        if (c.chunkBounds() == b)
        {
            if (c.depth() > depthEnd)
            {
                throw std::runtime_error("Invalid depth for subset box");
            }

            std::size_t id(c.chunkId().getSimple());
            std::size_t span(c.pointsPerChunk().getSimple());
            std::size_t depth(c.depth());

            while (depth < depthEnd)
            {
                id = (id << 2) + 1;
                span *= 4;
                ++depth;
            }

            spans.emplace(id, id + span);
        }
        else
        {
            throw std::runtime_error("Could not recreate subset box");
        }
    }

    if (spans.empty())
    {
        throw std::runtime_error("No spans found");
    }

    auto it(spans.begin());
    Span span(*it);
    spans.erase(it);

    for (const auto& s : spans)
    {
        span.merge(s);
    }

    std::vector<Span> results(depthEnd);

    for (std::size_t depth(depthEnd - 1); depth < depthEnd; --depth)
    {
        span.up();
        results[depth] = span;
    }

    return results;
}

} // namespace entwine

*/
