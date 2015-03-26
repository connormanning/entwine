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
#include <entwine/third/json/json.h>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>

namespace entwine
{

BaseBranch::BaseBranch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthEnd)
    : Branch(schema, dimensions, 0, depthEnd)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data()
    , m_locks(size())
{
    SimplePointTable table(schema);
    pdal::PointView view(table);

    for (std::size_t i(0); i < size(); ++i)
    {
        view.setField(pdal::Dimension::Id::X, i, INFINITY);
        view.setField(pdal::Dimension::Id::Y, i, INFINITY);
    }

    m_data = table.data();
}

BaseBranch::BaseBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data()
    , m_locks(size())
{
    load(path, meta);
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
            std::cout << "Race lost, recursing." << std::endl;
            lock.unlock();
            return addPoint(&toAdd, roller);
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
    if (!hasPoint(index))
    {
        throw std::runtime_error("Check if the point exists first");
    }

    return Point(*m_points[index].atom.load());
}

std::vector<char> BaseBranch::getPointData(const std::size_t index)
{
    if (hasPoint(index))
    {
        return std::vector<char>(getLocation(index), getLocation(index + 1));
    }
    else
    {
        return std::vector<char>();
    }
}

void BaseBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    meta["ids"].append(0);

    const std::string dataPath(path + "/0");
    std::ofstream dataStream(
            dataPath,
            std::ofstream::out | std::ofstream::trunc | std::ofstream::binary);
    if (!dataStream.good())
    {
        throw std::runtime_error("Could not open for write: " + dataPath);
    }

    const uint64_t uncompressedSize(m_data.size());

    std::unique_ptr<std::vector<char>> compressed(
            Compression::compress(
                m_data.data(),
                uncompressedSize,
                schema()));

    const uint64_t compressedSize(compressed->size());

    dataStream.write(
            reinterpret_cast<const char*>(&uncompressedSize),
            sizeof(uint64_t));
    dataStream.write(
            reinterpret_cast<const char*>(&compressedSize),
            sizeof(uint64_t));
    dataStream.write(compressed->data(), compressed->size());
    dataStream.close();
}

void BaseBranch::load(const std::string& path, const Json::Value& meta)
{
    const Json::Value jsonIds(meta["ids"]);

    // Only one ID allowed here, which is ID zero.
    if (!jsonIds.isArray() || jsonIds.size() != 1 || jsonIds[0] != 0)
    {
        throw std::runtime_error("Invalid serialized base branch.");
    }

    const std::string dataPath(path + "/0");
    std::ifstream dataStream(
            dataPath,
            std::ifstream::in | std::ifstream::binary);
    if (!dataStream.good())
    {
        throw std::runtime_error("Could not open for read: " + dataPath);
    }

    uint64_t uncSize(0), cmpSize(0);
    dataStream.read(reinterpret_cast<char*>(&uncSize), sizeof(uint64_t));
    dataStream.read(reinterpret_cast<char*>(&cmpSize), sizeof(uint64_t));

    std::vector<char> compressed(cmpSize);
    dataStream.read(compressed.data(), compressed.size());

    m_data = *Compression::decompress(compressed, schema(), uncSize).release();

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

