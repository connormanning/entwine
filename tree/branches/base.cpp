/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "base.hpp"

#include <limits>

#include "compression/util.hpp"
#include "types/point.hpp"
#include "tree/roller.hpp"
#include "tree/point-info.hpp"

namespace
{
    const double empty(std::numeric_limits<double>::max());
}

BaseBranch::BaseBranch(
        const Schema& schema,
        const std::size_t begin,
        const std::size_t end)
    : Branch(schema, begin, end)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data(new std::vector<char>(size() * schema.stride(), empty))
    , m_locks(size())
{ }

BaseBranch::BaseBranch(
        const Schema& schema,
        const std::size_t begin,
        const std::size_t end,
        std::vector<char>* data)
    : Branch(schema, begin, end)
    , m_points(size(), std::atomic<const Point*>(0))
    , m_data(data)
    , m_locks(size())
{
    double x(0);
    double y(0);

    const std::size_t stride(schema.stride());

    for (std::size_t i(0); i < m_data->size() / stride; ++i)
    {
        std::memcpy(
                reinterpret_cast<char*>(&x),
                m_data->data() + stride * i,
                sizeof(double));
        std::memcpy(
                reinterpret_cast<char*>(&y),
                m_data->data() + stride * i,
                sizeof(double));

        if (x != empty && y != empty)
        {
            m_points[i].atom.store(new Point(x, y));
        }
    }
}

BaseBranch::~BaseBranch()
{
    for (auto& p : m_points)
    {
        if (p.atom.load()) delete p.atom.load();
    }
}

bool BaseBranch::putPoint(PointInfo** toAddPtr, const Roller& roller)
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
            return putPoint(&toAdd, roller);
        }
    }

    return done;
}

const Point* BaseBranch::getPoint(std::size_t index) const
{
    return m_points[index].atom.load();
}

void BaseBranch::save(const std::string& dir, Json::Value& meta) const
{
    meta[std::to_string(begin())] = static_cast<Json::UInt64>(end());

    const std::string dataPath(dir + "/0");
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

void BaseBranch::load(const std::string& dir, const Json::Value& meta)
{
    const std::string dataPath(dir + "/0");
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
        std::memcpy(
                reinterpret_cast<char*>(&x),
                m_data->data() + stride * i,
                sizeof(double));
        std::memcpy(
                reinterpret_cast<char*>(&y),
                m_data->data() + stride * i,
                sizeof(double));

        if (x != empty && y != empty)
        {
            m_points[i].atom.store(new Point(x, y));
        }
    }
}

