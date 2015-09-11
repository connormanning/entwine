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
#include <entwine/types/range.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

Subset::Subset(
        const Structure& structure,
        const BBox* bbox,
        const std::size_t id,
        const std::size_t of)
    : m_structure(structure)
    , m_id(id - 1)
    , m_of(of)
    , m_sub()
    , m_minNullDepth(0)
{
    if (!id) throw std::runtime_error("Subset IDs should be 1-based.");

    if (bbox)
    {
        update(*bbox);
    }
}

Subset::Subset(
        const Structure& structure,
        const BBox& bbox,
        const Json::Value& json)
    : m_structure(structure)
    , m_id(json["id"].asUInt64() - 1)
    , m_of(json["of"].asUInt64())
    , m_sub()
    , m_minNullDepth(0)
{
    update(bbox);
}

Subset::Subset(const Subset& other)
    : m_structure(other.m_structure)
    , m_id(other.id())
    , m_of(other.of())
    , m_sub(new BBox(other.bbox()))
    , m_minNullDepth(other.minNullDepth())
{ }

Json::Value Subset::toJson() const
{
    Json::Value json;
    json["id"] = static_cast<Json::UInt64>(m_id + 1);
    json["of"] = static_cast<Json::UInt64>(m_of);
    return json;
}

void Subset::update(const BBox& bbox)
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

    m_minNullDepth = 1;
    std::size_t cap(factor);

    while (cap < m_of)
    {
        ++m_minNullDepth;
        cap *= factor;
    }

    const std::size_t boxes(cap / m_of);
    const std::size_t startOffset(m_id * boxes);

    const std::size_t iterations(ChunkInfo::logN(cap, factor));
    const std::size_t mask(0x3);

    for (std::size_t curId(startOffset); curId < startOffset + boxes; ++curId)
    {
        Climber climber(bbox, m_structure);
        for (std::size_t i(0); i < iterations; ++i)
        {
            Climber::Dir dir(
                    static_cast<Climber::Dir>(
                        curId >> (i * dimensions) & mask));

            switch (dir)
            {
                case Climber::Dir::swd: climber.goSwd(); break;
                case Climber::Dir::sed: climber.goSed(); break;
                case Climber::Dir::nwd: climber.goNwd(); break;
                case Climber::Dir::ned: climber.goNed(); break;
                default: throw std::runtime_error("Unexpected value");
            }
        }

        if (!m_sub) m_sub.reset(new BBox(climber.bbox()));
        else m_sub->grow(climber.bbox());
    }

    // For octrees, we've shrunken our Z coordinates - blow them back up to
    // span the whole set.
    m_sub->growZ(Range(bbox.min().z, bbox.max().z));
}

} // namespace entwine

