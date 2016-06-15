/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/registry.hpp>

#include <algorithm>

#include <pdal/PointView.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Registry::Registry(const Builder& builder, const bool exists)
    : m_builder(builder)
    , m_structure(m_builder.metadata().structure())
    , m_cold(makeUnique<Cold>(m_builder, exists))
{ }

Registry::~Registry()
{ }

void Registry::save() const
{
    if (m_cold) m_cold->save(m_builder.outEndpoint());
}

bool Registry::addPoint(
        Cell::PooledNode& cell,
        Climber& climber,
        Clipper& clipper,
        const std::size_t maxDepth)
{
    Tube::Insertion attempt;

    while (true)
    {
        attempt = m_cold->insert(climber, clipper, cell);

        if (!attempt.done())
        {
            if (attempt.delta()) climber.count(attempt.delta());

            if (
                    m_structure.inRange(climber.depth() + 1) &&
                    (!maxDepth || climber.depth() + 1 < maxDepth))
            {
                climber.magnify(cell->point());
            }
            else
            {
                return false;
            }
        }
        else
        {
            // TODO Should account for delta even if done - if we've swapped
            // delta may be greater than 1.
            climber.count();
            return true;
        }
    }
}

void Registry::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id)
{
    m_cold->clip(index, chunkNum, id);
}

void Registry::merge(const Registry& other)
{
    m_cold->merge(*other.m_cold);
}

} // namespace entwine

