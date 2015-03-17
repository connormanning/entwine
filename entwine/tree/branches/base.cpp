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

#include <entwine/compression/util.hpp>
#include <entwine/third/json/json.h>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>

namespace
{
    const double empty(std::numeric_limits<double>::max());
}

namespace entwine
{

BaseBranch::BaseBranch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthEnd)
    : Branch(schema, dimensions, 0, depthEnd)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data(new std::vector<char>(size() * schema.stride()))
    , m_locks(size())
{
    const std::size_t stride(schema.stride());

    for (std::size_t offset(0); offset < m_data->size(); offset += stride)
    {
        std::memcpy(
                m_data->data() + offset,
                &empty,
                sizeof(double));
        std::memcpy(
                m_data->data() + offset + sizeof(double),
                &empty,
                sizeof(double));
    }
}

BaseBranch::BaseBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data(new std::vector<char>(size() * schema.stride()))
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
                // Pull out the old stored value and store the Point that
                // was in our atomic member, so we can overwrite that with
                // the new one.
                PointInfo* old(
                        new PointInfo(
                            curPoint,
                            m_data->data() + index * schema().stride(),
                            toAdd->bytes.size()));

                // Store this point.
                toAdd->write(m_data->data() + index * schema().stride());
                myPoint.store(toAdd->point);
                delete toAdd;

                // Send our old stored value downstream.
                toAdd = old;
            }
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(m_locks[index]);
        if (!myPoint.load())
        {
            toAdd->write(m_data->data() + index * schema().stride());
            myPoint.store(toAdd->point);
            delete toAdd;
            done = true;
        }
        else
        {
            // Someone beat us here, call again to enter the other branch.
            // Be sure to unlock our mutex first.
            lock.unlock();
            return addPoint(&toAdd, roller);
        }
    }

    return done;
}

const Point* BaseBranch::getPoint(const std::size_t index)
{
    return m_points[index].atom.load();
}

std::vector<char> BaseBranch::getPointData(const std::size_t index)
{
    // TODO Since we are being queried, we are assuming that addPoint() is no
    // longer be called so the data is now constant.  Does this call need to be
    // supported during the build phase?  If so we need to lock here.
    if (getPoint(index))
    {
        const std::size_t pointSize(schema().stride());
        std::vector<char> bytes(pointSize);
        std::memcpy(bytes.data(), getPointPosition(index), pointSize);
        return bytes;
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

    const uint64_t uncompressedSize(m_data->size());

    std::unique_ptr<std::vector<char>> compressed(
            Compression::compress(
                *m_data.get(),
                schema().pointContext().dimTypes()));

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
    if (
            !meta["ids"].isArray() ||
            meta["ids"].size() != 1 ||
            meta[0] != 0)
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

    std::unique_ptr<std::vector<char>> uncompressed(
            Compression::decompress(
                compressed,
                schema().pointContext().dimTypes(),
                uncSize));

    double x(0);
    double y(0);

    const std::size_t stride(schema().stride());

    for (std::size_t i(0); i < m_data->size() / stride; ++i)
    {
        const char* pos(getPointPosition(i));
        std::memcpy(&x, pos, sizeof(double));
        std::memcpy(&y, pos + sizeof(double), sizeof(double));

        if (x != empty && y != empty)
        {
            m_points[i].atom.store(new Point(x, y));
        }
    }
}

char* BaseBranch::getPointPosition(const std::size_t index) const
{
    return m_data->data() + index * schema().stride();
}

} // namespace entwine

