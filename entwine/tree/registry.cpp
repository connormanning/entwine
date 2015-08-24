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

#include <entwine/third/json/json.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
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

    const std::size_t clipPoolSize(4);
    const std::size_t clipQueueSize(4);
}

Registry::Registry(
        arbiter::Endpoint& endpoint,
        const Schema& schema,
        const Structure& structure)
    : m_endpoint(endpoint)
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
        m_cold.reset(new Cold(endpoint, schema, m_structure, m_empty));
    }
}

Registry::Registry(
        arbiter::Endpoint& endpoint,
        const Schema& schema,
        const Structure& structure,
        const Json::Value& meta)
    : m_endpoint(endpoint)
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
                m_structure.baseIndexBegin().str() +
                m_structure.subsetPostfix());

        std::vector<char> data(m_endpoint.getSubpathBinary(basePath));
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
        m_cold.reset(new Cold(endpoint, schema, m_structure, m_empty, meta));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(PointInfo& toAdd, Climber& climber, Clipper* clipper)
{
    if (Entry* entry = getEntry(climber, clipper))
    {
        Point myPoint(entry->point());

        if (Point::exists(myPoint))
        {
            const Point& mid(climber.bbox().mid());

            if (better(toAdd.point, myPoint, mid, m_is3d))
            {
                // Reload point after locking.
                Locker locker(entry->getLocker());
                myPoint = entry->point();

                if (better(toAdd.point, myPoint, mid, m_is3d))
                {
                    const std::size_t pointSize(m_schema.pointSize());

                    // Store this point and send the old value downstream.
                    PointInfoDeep old(myPoint, entry->data(), pointSize);
                    entry->update(toAdd.point, toAdd.data, pointSize);
                    toAdd = old;
                }
            }
        }
        else
        {
            // Reload point after locking.
            std::unique_ptr<Locker> locker(new Locker(entry->getLocker()));
            myPoint = entry->point();

            if (!Point::exists(myPoint))
            {
                entry->update(toAdd.point, toAdd.data, m_schema.pointSize());
                return true;
            }
            else
            {
                std::cout << "Racing..." << std::endl;
                // Someone beat us here, call again to enter the other branch.
                // Be sure to release our lock first.
                locker.reset(0);
                return addPoint(toAdd, climber, clipper);
            }
        }
    }

    climber.magnify(toAdd.point);

    if (!m_structure.inRange(climber.index())) return false;

    return addPoint(toAdd, climber, clipper);
}

void Registry::save(Json::Value& meta)
{
    m_base->save(m_endpoint, m_structure.subsetPostfix());

    if (m_cold) meta["ids"] = m_cold->toJson();
}

Entry* Registry::getEntry(const Climber& climber, Clipper* clipper)
{
    Entry* entry(0);

    const Id& index(climber.index());

    if (m_structure.isWithinBase(index))
    {
        entry = m_base->getEntry(index);
    }
    else if (m_structure.isWithinCold(index))
    {
        entry = m_cold->getEntry(climber, clipper);
    }

    return entry;
}

void Registry::clip(
        const Id& index,
        const std::size_t chunkNum,
        Clipper* clipper)
{
    m_cold->clip(index, chunkNum, clipper, *m_pool);
}

} // namespace entwine

