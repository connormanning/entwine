/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "registry.hpp"

#include "tree/roller.hpp"
#include "types/bbox.hpp"
#include "point-info.hpp"

namespace
{
    const std::size_t dimensions(2);

    std::size_t getOffset(std::size_t depth)
    {
        std::size_t offset(0);

        for (std::size_t i(0); i < depth; ++i)
        {
            offset = (offset << dimensions) + 1;
        }

        return offset;
    }
}

Registry::Registry(
        std::size_t pointSize,
        std::size_t baseDepth,
        std::size_t flatDepth,
        std::size_t deadDepth)
    : m_pointSize(pointSize)
    , m_baseDepth(baseDepth)
    , m_flatDepth(flatDepth)
    , m_deadDepth(deadDepth)
    , m_baseOffset(getOffset(baseDepth))
    , m_flatOffset(getOffset(flatDepth))
    , m_deadOffset(getOffset(deadDepth))
    , m_basePoints(m_baseOffset, std::atomic<const Point*>(0))
    , m_baseData(new std::vector<char>(m_baseOffset * pointSize, 0))
    , m_baseLocks(m_baseOffset)
{
    if (baseDepth > flatDepth || flatDepth > deadDepth)
    {
        throw std::runtime_error("Invalid registry params");
    }
}

Registry::Registry(
        std::size_t pointSize,
        std::shared_ptr<std::vector<char>> data,
        std::size_t baseDepth,
        std::size_t flatDepth,
        std::size_t deadDepth)
    : m_pointSize(pointSize)
    , m_baseDepth(baseDepth)
    , m_flatDepth(flatDepth)
    , m_deadDepth(deadDepth)
    , m_baseOffset(getOffset(baseDepth))
    , m_flatOffset(getOffset(flatDepth))
    , m_deadOffset(getOffset(deadDepth))
    , m_basePoints(m_baseOffset, std::atomic<const Point*>(0))
    , m_baseData(data)
    , m_baseLocks(m_baseOffset)
{
    if (baseDepth > flatDepth || flatDepth > deadDepth)
    {
        throw std::runtime_error("Invalid registry params");
    }

    double x(0);
    double y(0);

    for (std::size_t i(0); i < m_baseData->size() / m_pointSize; ++i)
    {
        std::memcpy(
                reinterpret_cast<char*>(&x),
                m_baseData->data() + m_pointSize * i,
                sizeof(double));
        std::memcpy(
                reinterpret_cast<char*>(&y),
                m_baseData->data() + m_pointSize * i,
                sizeof(double));

        if (x != 0 && y != 0)
        {
            m_basePoints[i].atom.store(new Point(x, y));
        }
    }
}

Registry::~Registry()
{
    for (auto& p : m_basePoints)
    {
        if (p.atom.load()) delete p.atom.load();
    }
}

void Registry::put(
        PointInfo** toAddPtr,
        Roller& roller)
{
    bool done(false);

    PointInfo* toAdd(*toAddPtr);

    if (roller.depth() < m_baseDepth)
    {
        const std::size_t index(roller.pos());
        auto& myPoint(m_basePoints[index].atom);

        if (myPoint.load())
        {
            const Point mid(roller.bbox().mid());
            if (toAdd->point->sqDist(mid) < myPoint.load()->sqDist(mid))
            {
                std::lock_guard<std::mutex> lock(m_baseLocks[index]);
                const Point* curPoint(myPoint.load());

                // Reload the reference point now that we've acquired the lock.
                if (toAdd->point->sqDist(mid) < curPoint->sqDist(mid))
                {
                    if (roller.depth() < 2)
                        std::cout <<
                            "Storing (" <<
                            toAdd->point->x << "," << toAdd->point->y <<
                            ") at level " << roller.depth() << std::endl;

                    // Pull out the old stored value and store the Point that
                    // was in our atomic member, so we can overwrite that with
                    // the new one.
                    PointInfo* old(
                            new PointInfo(
                                curPoint,
                                m_baseData->data() + index * m_pointSize,
                                toAdd->bytes.size()));

                    // Store this point in the base data store.
                    toAdd->write(m_baseData->data() + index * m_pointSize);
                    myPoint.store(toAdd->point);
                    delete toAdd;

                    // Send our old stored value downstream.
                    toAdd = old;
                }
            }
        }
        else
        {
            std::unique_lock<std::mutex> lock(m_baseLocks[index]);
            if (!myPoint.load())
            {
                toAdd->write(m_baseData->data() + index * m_pointSize);
                myPoint.store(toAdd->point);
                delete toAdd;
                done = true;
            }
            else
            {
                // Someone beat us here, call again to enter the other branch.
                // Be sure to unlock our mutex first.
                lock.unlock();
                put(&toAdd, roller);
            }
        }
    }
    else if (roller.depth() < m_flatDepth) { /* TODO */ }
    else if (roller.depth() < m_deadDepth) { /* TODO */ }
    else
    {
        delete toAdd->point;
        delete toAdd;

        done = true;
    }

    if (!done)
    {
        roller.magnify(toAdd->point);
        put(&toAdd, roller);
    }
}

void Registry::getPoints(
        const Roller& roller,
        MultiResults& results,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    auto& myPoint(m_basePoints[roller.pos()].atom);

    if (myPoint.load())
    {
        if (
                (roller.depth() >= depthBegin) &&
                (roller.depth() < depthEnd || !depthEnd))
        {
            results.push_back(std::make_pair(0, roller.pos() * m_pointSize));
        }

        if (roller.depth() + 1 < depthEnd || !depthEnd)
        {
            getPoints(roller.getNw(), results, depthBegin, depthEnd);
            getPoints(roller.getNe(), results, depthBegin, depthEnd);
            getPoints(roller.getSw(), results, depthBegin, depthEnd);
            getPoints(roller.getSe(), results, depthBegin, depthEnd);
        }
    }
}

void Registry::getPoints(
        const Roller& roller,
        MultiResults& results,
        const BBox& query,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(query)) return;

    auto& myPoint(m_basePoints[roller.pos()].atom);
    if (myPoint.load())
    {
        if (
                (roller.depth() >= depthBegin) &&
                (roller.depth() < depthEnd || !depthEnd))
        {
            results.push_back(std::make_pair(0, roller.pos() * m_pointSize));
        }

        if (roller.depth() + 1 < depthEnd || !depthEnd)
        {
            getPoints(roller.getNw(), results, depthBegin, depthEnd);
            getPoints(roller.getNe(), results, depthBegin, depthEnd);
            getPoints(roller.getSw(), results, depthBegin, depthEnd);
            getPoints(roller.getSe(), results, depthBegin, depthEnd);
        }
    }
}

