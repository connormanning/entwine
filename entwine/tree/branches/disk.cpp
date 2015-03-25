/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/disk.hpp>

#include <pdal/PointView.hpp>

#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/platform.hpp>

namespace entwine
{

namespace
{
    const double empty(std::numeric_limits<double>::max());

    std::size_t pointsPerChunk(std::size_t depthBegin, std::size_t dimensions)
    {
        return
            Branch::calcOffset(depthBegin + 1, dimensions) -
            Branch::calcOffset(depthBegin, dimensions);
    }

    std::vector<char> makeEmptyChunk(
            const Schema& schema,
            std::size_t pointsPerChunk)
    {
        SimplePointTable table(schema);
        pdal::PointView view(table);

        for (std::size_t i(0); i < pointsPerChunk; ++i)
        {
            view.setField(pdal::Dimension::Id::X, i, empty);
            view.setField(pdal::Dimension::Id::Y, i, empty);
        }

        return table.data();
    }

    const std::ios_base::openmode binaryTruncMode(
            std::ofstream::binary |
            std::ofstream::out |
            std::ofstream::trunc);
}

Chunk::Chunk(
        const std::string& path,
        std::size_t begin,
        const std::vector<char>& initData)
{
    // TODO Check if it exists first for the serial awakening case.

    std::string filename(path + "/" + std::to_string(begin));
    Fs::writeFile(filename, initData, binaryTruncMode);
}

bool Chunk::addPoint(const PointInfo* toAdd)
{
    delete toAdd->point;
    delete toAdd;
    return true;
}

LockedChunk::~LockedChunk()
{
    if (exists())
    {
        delete m_chunk.load();
    }
}

void LockedChunk::init(
        const std::string& path,
        const std::size_t begin,
        const std::vector<char>& initData)
{
    if (!exists())
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!exists())
        {
            m_chunk.store(new Chunk(path, begin, initData));
        }
    }
}

bool LockedChunk::exists() const
{
    return m_chunk.load() != 0;
}

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(schema, dimensions, depthBegin, depthEnd)
    , m_path(path)
    , m_pointsPerChunk(pointsPerChunk(depthBegin, dimensions))
    , m_chunks((depthEnd - depthEnd) / m_pointsPerChunk)
    , m_emptyChunk(makeEmptyChunk(schema, m_pointsPerChunk))
{ }

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_path(path)
    , m_pointsPerChunk(pointsPerChunk(depthBegin(), dimensions))
    , m_chunks((depthEnd() - depthEnd()) / m_pointsPerChunk)
    , m_emptyChunk(makeEmptyChunk(schema, m_pointsPerChunk))
{ }

DiskBranch::~DiskBranch()
{ }

bool DiskBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    std::size_t chunkId(getChunkId(roller.pos()));
    LockedChunk& lockedChunk(m_chunks[chunkId]);

    if (!lockedChunk.exists())
    {
        // TODO This is relatively infrequent, so could lock here and add this
        // chunkId to some structure for bookkeeping.
        //
        // If that structure is full, put stalest entry to sleep.

        lockedChunk.init(m_path, chunkId, m_emptyChunk);
    }

    // At this point, the LockedChunk's member is created, and its backing
    // file exists, although it might be asleep (i.e. not mem-mapped).
    Chunk& chunk(lockedChunk.get());

    return chunk.addPoint(*toAddPtr);
}

bool DiskBranch::hasPoint(std::size_t index)
{
    bool has(false);

    if (m_chunks[getChunkId(index)].exists())
    {

    }

    return has;
}

Point DiskBranch::getPoint(std::size_t index)
{
    return Point();
}

std::vector<char> DiskBranch::getPointData(
        std::size_t index,
        const Schema& schema)
{
    return std::vector<char>();
}

std::size_t DiskBranch::getChunkId(std::size_t index) const
{
    assert(index >= indexBegin() && index < indexEnd());

    const std::size_t offset(index - indexBegin());
    return offset / m_pointsPerChunk;
}

void DiskBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    // TODO
    /*
    meta["ids"]
    */
}

} // namespace entwine

