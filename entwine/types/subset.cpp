/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/subset.hpp>

#include <entwine/tree/climber.hpp>
#include <entwine/tree/hierarchy.hpp>

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

    split(bounds);
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

void Subset::split(const Bounds& bounds)
{
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

} // namespace entwine

