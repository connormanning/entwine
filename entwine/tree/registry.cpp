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
    , m_base()
    , m_cold()
    , m_pool(new Pool(clipPoolSize, clipQueueSize))
    , m_empty(makeEmpty(m_schema, m_structure.chunkPoints()))
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

    if (m_structure.coldIndexSpan())
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
    , m_base()
    , m_cold()
    , m_pool(new Pool(clipPoolSize, clipQueueSize))
    , m_empty(makeEmpty(m_schema, m_structure.chunkPoints()))
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

    if (m_structure.coldIndexSpan())
    {
        m_cold.reset(new Cold(source, schema, m_structure, m_empty, meta));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper)
{
    bool accepted(false);

    const std::size_t index(roller.index());

    if (Entry* entry = getEntry(index, clipper))
    {
        PointInfo* toAdd(*toAddPtr);
        std::atomic<const Point*>& myPoint(entry->point());

        if (myPoint.load())
        {
            const Point& mid(roller.bbox().mid());

            if (toAdd->point->sqDist(mid) < myPoint.load()->sqDist(mid))
            {
                std::lock_guard<std::mutex> lock(entry->mutex());
                const Point* curPoint(myPoint.load());

                if (toAdd->point->sqDist(mid) < curPoint->sqDist(mid))
                {
                    const std::size_t pointSize(m_schema.pointSize());

                    // Pull out the old stored value and store the Point that
                    // was in our atomic member, so we can overwrite that with
                    // the new one.
                    PointInfo* old(
                            new PointInfoDeep(
                                curPoint,
                                entry->data(),
                                pointSize));

                    // Store this point.
                    toAdd->write(entry->data(), pointSize);
                    myPoint.store(toAdd->point);
                    delete toAdd;

                    // Send our old stored value downstream.
                    *toAddPtr = old;
                }
            }
        }
        else
        {
            std::unique_lock<std::mutex> lock(entry->mutex());
            if (!myPoint.load())
            {
                toAdd->write(entry->data(), m_schema.pointSize());
                myPoint.store(toAdd->point);
                delete toAdd;
                accepted = true;
            }
            else
            {
                // Someone beat us here, call again to enter the other branch.
                // Be sure to unlock our mutex first.
                lock.unlock();
                return addPoint(toAddPtr, roller, clipper);
            }
        }
    }

    if (!accepted)
    {
        roller.magnify((*toAddPtr)->point);

        if (m_structure.inRange(roller.index()))
        {
            accepted = addPoint(toAddPtr, roller, clipper);
        }
        else
        {
            // This point fell beyond the bottom of the tree.
            delete (*toAddPtr)->point;
            delete (*toAddPtr);
        }
    }

    return accepted;
}

void Registry::save(Json::Value& meta)
{
    m_base->save(m_source, m_structure.subsetPostfix());

    if (m_cold) meta["ids"] = m_cold->toJson();
}

Entry* Registry::getEntry(const std::size_t index, Clipper* clipper)
{
    Entry* entry(0);

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

