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
#include <entwine/types/range.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

Subset::Subset(
        Structure& structure,
        const BBox& bbox,
        const std::size_t id,
        const std::size_t of)
    : m_id(id - 1)
    , m_of(of)
    , m_sub()
{
    if (!id) throw std::runtime_error("Subset IDs should be 1-based.");

    split(structure, bbox);
}

Subset::Subset(
        Structure& structure,
        const BBox& bbox,
        const Json::Value& json)
    : m_id(json["id"].asUInt64() - 1)
    , m_of(json["of"].asUInt64())
    , m_sub()
{
    split(structure, bbox);
}

Json::Value Subset::toJson() const
{
    Json::Value json;

    json["id"] = static_cast<Json::UInt64>(m_id + 1);
    json["of"] = static_cast<Json::UInt64>(m_of);

    return json;
}

void Subset::split(Structure& structure, const BBox& bbox)
{
    if (m_of <= 1 || m_of > 64)
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

    std::size_t minNullDepth(1);
    std::size_t cap(factor);

    while (cap < m_of)
    {
        ++minNullDepth;
        cap *= factor;
    }

    const std::size_t boxes(cap / m_of);
    const std::size_t startOffset(m_id * boxes);

    const std::size_t iterations(ChunkInfo::logN(cap, factor));
    const std::size_t mask(0x3);

    bool set(false);

    for (std::size_t curId(startOffset); curId < startOffset + boxes; ++curId)
    {
        Climber climber(bbox, structure);
        for (std::size_t i(iterations - 1); i < iterations; --i)
        {
            Dir dir(static_cast<Dir>(curId >> (i * dimensions) & mask));

            switch (dir)
            {
                case Dir::swd: climber.goSwd(); break;
                case Dir::sed: climber.goSed(); break;
                case Dir::nwd: climber.goNwd(); break;
                case Dir::ned: climber.goNed(); break;
                default: throw std::runtime_error("Unexpected value");
            }
        }

        if (!set)
        {
            m_sub = climber.bbox();
            set = true;
        }
        else
        {
            m_sub.grow(climber.bbox());
        }
    }

    // For octrees, we've shrunken our Z coordinates - blow them back up to
    // span the whole set.
    m_sub.growZ(Range(bbox.min().z, bbox.max().z));

    structure.accomodateSubset(*this, minNullDepth);
}

} // namespace entwine

