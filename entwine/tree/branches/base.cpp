/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/base.hpp>

#include <cstring>
#include <limits>

#include <pdal/PointView.hpp>
#include <pdal/PointTable.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/http/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/util/pool.hpp>

namespace
{
    std::vector<char> makeEmptyPoint(const entwine::Schema& schema)
    {
        entwine::SimplePointTable table(schema);
        pdal::PointView view(table);

        view.setField(pdal::Dimension::Id::X, 0, 0);//INFINITY);
        view.setField(pdal::Dimension::Id::Y, 0, 0);//INFINITY);

        return table.data();
    }
}

namespace entwine
{

BaseBranch::BaseBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthEnd)
    : Branch(source, schema, dimensions, 0, depthEnd)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data()
    , m_locks(size())
{
    SimplePointTable table(schema);
    pdal::PointView view(table);

    for (std::size_t i(0); i < size(); ++i)
    {
        view.setField(pdal::Dimension::Id::X, i, 0);//INFINITY);
        view.setField(pdal::Dimension::Id::Y, i, 0);//INFINITY);
    }

    m_data = table.data();
}

BaseBranch::BaseBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(source, schema, dimensions, meta)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data()
    , m_locks(size())
{
    load(meta);
}

BaseBranch::~BaseBranch()
{
    for (auto& p : m_points)
    {
        if (p.atom.load()) delete p.atom.load();
    }
}

bool BaseBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    bool done(false);
    PointInfo* toAdd(*toAddPtr);
    const std::size_t index(roller.pos());
    auto& myPoint(m_points[index].atom);

    if (myPoint.load())
    {
        const Point mid(roller.bbox().mid());

        if (toAdd->point->sqDist(mid) < myPoint.load()->sqDist(mid))
        {
            std::lock_guard<std::mutex> lock(m_locks[index]);
            const Point* curPoint(myPoint.load());

            if (toAdd->point->sqDist(mid) < curPoint->sqDist(mid))
            {
                char* pos(getLocation(index));

                // Pull out the old stored value and store the Point that
                // was in our atomic member, so we can overwrite that with
                // the new one.
                PointInfo* old(
                        new PointInfo(
                            curPoint,
                            pos,
                            schema().pointSize()));

                // Store this point.
                toAdd->write(pos);
                myPoint.store(toAdd->point);
                delete toAdd;

                // Send our old stored value downstream.
                *toAddPtr = old;
            }
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(m_locks[index]);
        if (!myPoint.load())
        {
            toAdd->write(getLocation(index));
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

bool BaseBranch::hasPoint(const std::size_t index)
{
    return m_points[index].atom.load() != 0;
}

Point BaseBranch::getPoint(const std::size_t index)
{
    Point point;//(INFINITY, INFINITY);

    if (hasPoint(index))
    {
        point = Point(*m_points[index].atom.load());
    }

    return point;
}

std::vector<char> BaseBranch::getPointData(const std::size_t index)
{
    std::vector<char> data;

    if (hasPoint(index))
    {
        data.assign(getLocation(index), getLocation(index + 1));
    }

    return data;
}

void BaseBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    const std::size_t pointSize(schema().pointSize());
    const std::vector<char> emptyPoint(makeEmptyPoint(schema()));

    for (std::size_t id(start); id < indexEnd(); id += chunkPoints)
    {
        bool populated(false);
        std::size_t current(id);
        const std::size_t end(id + chunkPoints);

        while (!populated && current < end)
        {
            if (hasPoint(current++))
            {
                populated = true;
            }
        }

        if (populated)
        {
            std::cout << "Base ID " << id << std::endl;
            ids.push_back(id);

            // TODO Pool.
            //pool.add([this, &output, id, chunkPoints, pointSize, &emptyPoint]()
            //{
                std::size_t offset(0);
                std::vector<char> data(chunkPoints * pointSize);

                for (std::size_t i(id); i < id + chunkPoints; ++i)
                {
                    std::vector<char> point(getPointData(i));

                    std::memcpy(
                            data.data() + offset,
                            point.size() ? point.data() : emptyPoint.data(),
                            pointSize);

                    offset += pointSize;
                }

                auto compressed(Compression::compress(data, schema()));
                output.put(std::to_string(id), *compressed);

            //});
        }
    }

    // pool.join();

    m_points.clear();
    m_data.clear();
    m_locks.clear();
}

void BaseBranch::saveImpl(Json::Value& meta)
{
    meta["ids"].append(0);

    const uint64_t uncSize(m_data.size());

    std::unique_ptr<std::vector<char>> compressed(
            Compression::compress(
                m_data.data(),
                uncSize,
                schema()));

    const uint64_t cmpSize(compressed->size());
    compressed->insert(
            compressed->end(),
            &cmpSize,
            &cmpSize + sizeof(uint64_t));

    m_source.put("0", *compressed);
}

void BaseBranch::load(const Json::Value& meta)
{
    const Json::Value jsonIds(meta["ids"]);

    // Only one ID allowed here, which is ID zero.
    if (!jsonIds.isArray() || jsonIds.size() != 1 || jsonIds[0] != 0)
    {
        throw std::runtime_error("Invalid serialized base branch.");
    }

    std::vector<char> data(m_source.get("0"));

    if (data.size() < sizeof(uint64_t))
    {
        throw std::runtime_error("Invalid base branch data.");
    }

    uint64_t uncSize(0);
    std::memcpy(
            &uncSize,
            data.data() + data.size() - sizeof(uint64_t),
            sizeof(uint64_t));
    data.resize(data.size() - sizeof(uint64_t));

    m_data = *Compression::decompress(data, schema(), uncSize).release();

    double x(0);
    double y(0);

    for (std::size_t i(0); i < size(); ++i)
    {
        char* pos(getLocation(i));

        SinglePointTable table(schema(), pos);
        LinkingPointView view(table);

        x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
        y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

        if (Point::exists(x, y))
        {
            m_points[i].atom.store(new Point(x, y));
        }
    }
}

char* BaseBranch::getLocation(std::size_t index)
{
    return m_data.data() + index * schema().pointSize();
}

} // namespace entwine

