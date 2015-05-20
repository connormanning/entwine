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

#include <entwine/drivers/source.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

Registry::Registry(
        Source& source,
        const Schema& schema,
        const Structure& structure)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_base()
    , m_cold()
{
    if (m_structure.baseIndexSpan())
    {
        m_base.reset(
                new Chunk(
                    m_schema,
                    m_structure.baseIndexBegin(),
                    m_structure.baseIndexEnd(),
                    true));
    }

    if (m_structure.coldIndexSpan())
    {
        m_cold.reset(new Cold(source, schema, m_structure));
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
{
    if (m_structure.baseIndexSpan())
    {
        m_base.reset(
                new Chunk(
                    m_schema,
                    m_structure.baseIndexBegin(),
                    m_structure.baseIndexEnd(),
                    m_source.get(std::to_string(
                            m_structure.baseIndexBegin()))));
    }

    if (m_structure.coldIndexSpan())
    {
        m_cold.reset(new Cold(source, schema, m_structure, meta));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper)
{
    bool accepted(false);

    if (tryAdd(toAddPtr, roller, clipper))
    {
        accepted = true;
    }
    else
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

bool Registry::tryAdd(
        PointInfo** toAddPtr,
        const Roller& roller,
        Clipper* clipper)
{
    bool done(false);

    const std::size_t index(roller.index());

    if (Entry* entry = getEntry(index, clipper))
    {
        PointInfo* toAdd(*toAddPtr);
        std::atomic<const Point*>& myPoint(entry->point());

        if (myPoint.load())
        {
            const Point mid(roller.bbox().mid());

            if (toAdd->point->sqDist(mid) < myPoint.load()->sqDist(mid))
            {
                std::lock_guard<std::mutex> lock(entry->mutex());
                const Point* curPoint(myPoint.load());

                if (toAdd->point->sqDist(mid) < curPoint->sqDist(mid))
                {
                    // Pull out the old stored value and store the Point that
                    // was in our atomic member, so we can overwrite that with
                    // the new one.
                    PointInfo* old(
                            new PointInfo(
                                curPoint,
                                entry->data(),
                                m_schema.pointSize()));

                    // Store this point.
                    toAdd->write(entry->data());
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
                toAdd->write(entry->data());
                myPoint.store(toAdd->point);
                delete toAdd;
                done = true;
            }
            else
            {
                // Someone beat us here, call again to enter the other branch.
                // Be sure to unlock our mutex first.
                lock.unlock();
                done = tryAdd(toAddPtr, roller, clipper);
            }
        }
    }

    return done;
}


void Registry::save(Json::Value& meta)
{
    m_base->save(m_source);

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
    m_cold->clip(index, clipper);
}

} // namespace entwine

