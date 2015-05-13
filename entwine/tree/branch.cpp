/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branch.hpp>

#include <entwine/http/s3.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/branches/chunk.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

namespace
{
    std::set<std::size_t> loadIds(const Json::Value& meta)
    {
        std::set<std::size_t> ids;
        const Json::Value jsonIds(meta["ids"]);

        if (!jsonIds.isArray())
        {
            throw std::runtime_error("Invalid saved branch.");
        }

        for (std::size_t i(0); i < jsonIds.size(); ++i)
        {
            ids.insert(jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
        }

        return ids;
    }
}

Branch::Branch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : m_source(source)
    , m_ids()
    , m_schema(schema)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_indexBegin(calcOffset(m_depthBegin, dimensions))
    , m_indexEnd(calcOffset(m_depthEnd, dimensions))
    , m_dimensions(dimensions)
{ }

Branch::Branch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : m_source(source)
    , m_ids(loadIds(meta))
    , m_schema(schema)
    , m_depthBegin(meta["depthBegin"].asUInt64())
    , m_depthEnd(meta["depthEnd"].asUInt64())
    , m_indexBegin(calcOffset(m_depthBegin, dimensions))
    , m_indexEnd(calcOffset(m_depthEnd, dimensions))
    , m_dimensions(dimensions)
{ }

Branch::~Branch()
{ }

bool Branch::accepts(Clipper* clipper, const std::size_t index)
{
    assert(clipper);
    const bool accepted((index >= m_indexBegin) && (index < m_indexEnd));

    if (accepted && clipper)
    {
        grow(clipper, index);
    }

    return accepted;
}

bool Branch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    bool done(false);

    PointInfo* toAdd(*toAddPtr);
    const std::size_t index(roller.index());

    Entry& entry(getEntry(index));

    std::atomic<const Point*>& myPoint(entry.point());

    if (myPoint.load())
    {
        const Point mid(roller.bbox().mid());

        if (toAdd->point->sqDist(mid) < myPoint.load()->sqDist(mid))
        {
            std::lock_guard<std::mutex> lock(entry.mutex());
            const Point* curPoint(myPoint.load());

            if (toAdd->point->sqDist(mid) < curPoint->sqDist(mid))
            {
                // Pull out the old stored value and store the Point that
                // was in our atomic member, so we can overwrite that with
                // the new one.
                PointInfo* old(
                        new PointInfo(
                            curPoint,
                            entry.data(),
                            schema().pointSize()));

                // Store this point.
                toAdd->write(entry.data());
                myPoint.store(toAdd->point);
                delete toAdd;

                // Send our old stored value downstream.
                *toAddPtr = old;
            }
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(entry.mutex());
        if (!myPoint.load())
        {
            toAdd->write(entry.data());
            myPoint.store(toAdd->point);
            delete toAdd;
            done = true;
        }
        else
        {
            // Someone beat us here, call again to enter the other branch.
            // Be sure to unlock our mutex first.
            lock.unlock();
            done = addPoint(toAddPtr, roller);
        }
    }

    return done;
}

void Branch::save(Json::Value& meta)
{
    meta["depthBegin"] = static_cast<Json::UInt64>(m_depthBegin);
    meta["depthEnd"] = static_cast<Json::UInt64>(m_depthEnd);

    for (const std::size_t id : m_ids)
    {
        meta["ids"].append(static_cast<Json::UInt64>(id));
    }

    saveImpl(meta);
}

void Branch::finalize(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t chunkSize)
{
    const std::size_t ourStart(std::max(indexBegin(), start));
    finalizeImpl(output, pool, ids, ourStart, chunkSize);
}

std::size_t Branch::calcOffset(std::size_t depth, std::size_t dimensions)
{
    std::size_t offset(0);

    for (std::size_t i(0); i < depth; ++i)
    {
        offset = (offset << dimensions) + 1;
    }

    return offset;
}

const Schema& Branch::schema() const
{
    return m_schema;
}

std::size_t Branch::depthBegin() const
{
    return m_depthBegin;
}

std::size_t Branch::depthEnd() const
{
    return m_depthEnd;
}

std::size_t Branch::indexBegin() const
{
    return m_indexBegin;
}

std::size_t Branch::indexEnd() const
{
    return m_indexEnd;
}

std::size_t Branch::indexSpan() const
{
    return m_indexEnd - m_indexBegin;
}

std::size_t Branch::dimensions() const
{
    return m_dimensions;
}

} //namespace entwine

