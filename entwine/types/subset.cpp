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

namespace entwine
{

Subset::Subset(
        const std::size_t id,
        const std::size_t of,
        const std::size_t dimensions,
        const BBox* bbox)
    : m_id(id - 1)
    , m_of(of)
    , m_sub()
{
    if (!id) throw std::runtime_error("Subset IDs should be 1-based.");

    if (bbox)
    {
        update(*bbox);
    }
}

Subset::Subset(const Json::Value& json, const BBox& bbox)
    : m_id(json["id"].asUInt64())
    , m_of(json["of"].asUInt64())
    , m_sub()
{
    update(bbox);
}

Subset::Subset(const Subset& other)
    : m_id(other.id())
    , m_of(other.of())
    , m_sub(new BBox(other.bbox()))
{ }

Subset& Subset::operator=(const Subset& other)
{
    m_id = other.id();
    m_of = other.of();
    m_sub.reset(new BBox(other.bbox()));

    return *this;
}

void Subset::update(const BBox& bbox)
{
    /*
    std::unique_ptr<BBox> result;

    Climber climber(full, *this);
    std::size_t times(0);

    // TODO
    if (is3d()) throw std::runtime_error("Can't currently split octree");

    // TODO Very temporary.
    if (m_subset.second == 4) times = 1;
    else if (m_subset.second == 16) times = 2;
    else if (m_subset.second == 64) times = 3;
    else throw std::runtime_error("Invalid subset split");

    if (times)
    {
        for (std::size_t i(0); i < times; ++i)
        {
            Climber::Dir dir(
                    static_cast<Climber::Dir>(
                        m_subset.first >> (i * 2) & 0x03));

            if (dir == Climber::Dir::nwd) climber.goNwd();
            else if (dir == Climber::Dir::ned) climber.goNed();
            else if (dir == Climber::Dir::swd) climber.goSwd();
            else climber.goSed();
        }

        result.reset(new BBox(climber.bbox()));
    }
    else
    {
        throw std::runtime_error("Invalid magnification subset");
    }

    return result;
    */
}

} // namespace entwine

