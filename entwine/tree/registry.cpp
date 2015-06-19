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

#include <entwine/drivers/source.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    std::vector<char> makeEmpty(const Schema& schema, std::size_t numPoints)
    {
        SimplePointTable table(schema);
        pdal::PointView view(table);

        const auto emptyCoord(Point::emptyCoord());

        for (std::size_t i(0); i < numPoints; ++i)
        {
            view.setField(pdal::Dimension::Id::X, i, emptyCoord);
            view.setField(pdal::Dimension::Id::Y, i, emptyCoord);
        }

        return table.data();
    }

    bool better(
            const Point& candidate,
            const Point& current,
            const Point& goal,
            const bool is3d)
    {
        if (is3d)
        {
            return candidate.sqDist3d(goal) < current.sqDist3d(goal);
        }
        else
        {
            return candidate.sqDist2d(goal) < current.sqDist2d(goal);
        }
    }

    const std::size_t clipPoolSize(32);
    const std::size_t clipQueueSize(16);
}

Registry::Registry(
        Source& source,
        const Schema& schema,
        const Structure& structure)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_is3d(structure.is3d())
    , m_base()
    , m_cold()
    , m_pool(new Pool(clipPoolSize, clipQueueSize))
    , m_empty(makeEmpty(m_schema, m_structure.baseChunkPoints()))
{
    if (m_structure.baseIndexSpan())
    {
        m_base.reset(
                new ContiguousChunkData(
                    m_schema,
                    m_structure.baseIndexBegin(),
                    m_structure.baseIndexSpan(),
                    m_empty));
    }

    if (m_structure.hasCold())
    {
        m_cold.reset(new Cold(source, schema, m_structure, m_empty));
    }
}

Registry::Registry(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const Json::Value& meta)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_is3d(structure.is3d())
    , m_base()
    , m_cold()
    , m_pool(new Pool(clipPoolSize, clipQueueSize))
    , m_empty(makeEmpty(m_schema, m_structure.baseChunkPoints()))
{
    if (m_structure.baseIndexSpan())
    {
        const std::string basePath(
                std::to_string(m_structure.baseIndexBegin()) +
                m_structure.subsetPostfix());

        std::vector<char> data(m_source.get(basePath));
        ChunkType type(Chunk::getType(data));

        if (type != Contiguous)
        {
            throw std::runtime_error("Invalid base chunk type");
        }

        m_base.reset(
                new ContiguousChunkData(
                    m_schema,
                    m_structure.baseIndexBegin(),
                    m_structure.baseIndexSpan(),
                    data));
    }

    if (m_structure.hasCold())
    {
        m_cold.reset(new Cold(source, schema, m_structure, m_empty, meta));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(PointInfo& toAdd, Roller& roller, Clipper* clipper)
{
    bool accepted(false);

    if (Entry* entry = getEntry(roller, clipper))
    {
        std::atomic<Point>& myPoint(entry->point());

        if (Point::exists(myPoint.load()))
        {
            const Point& mid(roller.bbox().mid());

            if (better(toAdd.point, myPoint.load(), mid, m_is3d))
            {
                Locker locker(entry->getLocker());
                const Point& curPoint(myPoint.load());

                if (better(toAdd.point, curPoint, mid, m_is3d))
                {
                    const std::size_t pointSize(m_schema.pointSize());

                    PointInfoDeep old(curPoint, entry->data(), pointSize);

                    // Store this point.
                    toAdd.write(entry->data(), pointSize);
                    myPoint.store(toAdd.point);

                    // Send our old stored value downstream.
                    toAdd = old;
                }
            }
        }
        else
        {
            std::unique_ptr<Locker> locker(new Locker(entry->getLocker()));

            if (!Point::exists(myPoint.load()))
            {
                toAdd.write(entry->data(), m_schema.pointSize());
                myPoint.store(toAdd.point);
                accepted = true;
            }
            else
            {
                // Someone beat us here, call again to enter the other branch.
                // Be sure to release our lock first.
                locker.reset(0);
                return addPoint(toAdd, roller, clipper);
            }
        }
    }

    if (!accepted)
    {
        roller.magnify(toAdd.point);

        if (m_structure.inRange(roller.index()))
        {
            accepted = addPoint(toAdd, roller, clipper);
        }
    }

    return accepted;
}

void Registry::save(Json::Value& meta)
{
    m_base->save(m_source, m_structure.subsetPostfix());

    if (m_cold) meta["ids"] = m_cold->toJson();
}

Entry* Registry::getEntry(const Roller& roller, Clipper* clipper)
{
    Entry* entry(0);

    const std::size_t index(roller.index());

    if (m_structure.isWithinBase(index))
    {
        entry = m_base->getEntry(index);
    }
    else if (m_structure.isWithinCold(index))
    {
        entry = m_cold->getEntry(index, clipper);
    }

    return entry;
}

void Registry::clip(const std::size_t index, Clipper* clipper)
{
    m_cold->clip(index, clipper, *m_pool);
}

} // namespace entwine

