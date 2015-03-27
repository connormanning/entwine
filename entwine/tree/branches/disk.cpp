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

#include <sys/mman.h>

#include <pdal/PointView.hpp>

#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/platform.hpp>

namespace entwine
{

namespace
{
    std::size_t pointsPerChunk(std::size_t depthBegin, std::size_t dimensions)
    {
        return
            Branch::calcOffset(depthBegin + 1, dimensions) -
            Branch::calcOffset(depthBegin, dimensions);
    }

    std::size_t numChunks(
            std::size_t depthBegin,
            std::size_t depthEnd,
            std::size_t dimensions)
    {
        const std::size_t numPoints(
                Branch::calcOffset(depthEnd, dimensions) -
                Branch::calcOffset(depthBegin, dimensions));

        return numPoints / pointsPerChunk(depthBegin, dimensions);
    }

    std::vector<char> makeEmptyChunk(
            const Schema& schema,
            std::size_t pointsPerChunk)
    {
        SimplePointTable table(schema);
        pdal::PointView view(table);

        for (std::size_t i(0); i < pointsPerChunk; ++i)
        {
            view.setField(pdal::Dimension::Id::X, i, INFINITY);
            view.setField(pdal::Dimension::Id::Y, i, INFINITY);
        }

        return table.data();
    }

    std::string getFilename(const std::string& path, const std::size_t id)
    {
        return path + "/" + std::to_string(id);
    }

    const std::ios_base::openmode binaryTruncMode(
            std::ofstream::binary |
            std::ofstream::out |
            std::ofstream::trunc);
}

Chunk::Chunk(
        const Schema& schema,
        const std::string& path,
        const std::size_t begin,
        const std::vector<char>& initData)
    : m_schema(schema)
    , m_fd()
    , m_mapping(0)
    , m_begin(begin)
    , m_size(initData.size())
    , m_mutex()
{
    std::cout << "Making chunk" << std::endl;
    std::string filename(getFilename(path, m_begin));

    if (!fs::fileExists(filename))
    {
        fs::writeFile(filename, initData, binaryTruncMode);
    }

    m_fd.reset(new fs::FileDescriptor(filename));

    if (!m_fd->good())
    {
        throw std::runtime_error("Could not open " + filename);
    }

    char* mapping(
            static_cast<char*>(mmap(
                0,
                m_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                m_fd->id(),
                0)));

    if (mapping == MAP_FAILED)
        throw std::runtime_error("Could not map " + filename);

    m_mapping = mapping;
}

Chunk::Chunk(
        const Schema& schema,
        const std::string& path,
        const std::size_t begin,
        const std::size_t size)
    : m_schema(schema)
    , m_fd()
    , m_mapping(0)
    , m_begin(begin)
    , m_size(size)
    , m_mutex()
{
    std::string filename(getFilename(path, m_begin));

    if (!fs::fileExists(filename))
    {
        throw std::runtime_error("File does not exist");
    }

    m_fd.reset(new fs::FileDescriptor(filename));

    if (!m_fd->good())
    {
        throw std::runtime_error("Could not open " + filename);
    }

    char* mapping(
            static_cast<char*>(mmap(
                0,
                m_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                m_fd->id(),
                0)));

    if (mapping == MAP_FAILED)
        throw std::runtime_error("Could not map " + filename);

    m_mapping = mapping;
}

Chunk::~Chunk()
{
    sync();
}

bool Chunk::addPoint(const Roller& roller, PointInfo** toAddPtr)
{
    bool done(false);

    PointInfo* toAdd(*toAddPtr);

    char* pos(m_mapping + getByteOffset(roller.pos()));

    SinglePointTable table(m_schema, pos);
    LinkingPointView view(table);

    // TODO Should have multiple for high-traffic chunks close to the base.
    std::lock_guard<std::mutex> lock(m_mutex);

    double x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
    double y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

    if (Point::exists(x, y))
    {
        const Point curPoint(x, y);
        const Point mid(roller.bbox().mid());

        if (toAdd->point->sqDist(mid) < curPoint.sqDist(mid))
        {
            // Pull out the old stored value.
            PointInfo* old(
                    new PointInfo(
                        new Point(curPoint),
                        pos,
                        m_schema.pointSize()));

            toAdd->write(pos);
            delete toAdd->point;
            delete toAdd;

            *toAddPtr = old;
        }
    }
    else
    {
        // Empty point here - store incoming.
        toAdd->write(pos);

        delete toAdd->point;
        delete toAdd;
        done = true;
    }

    return done;
}

bool Chunk::hasPoint(const std::size_t index) const
{
    const Point p(getPoint(index));
    return Point::exists(p.x, p.y);
}

Point Chunk::getPoint(const std::size_t index) const
{
    char* pos(m_mapping + getByteOffset(index));

    SinglePointTable table(m_schema, pos);
    LinkingPointView view(table);

    double x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
    double y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

    return Point(x, y);
}

std::vector<char> Chunk::getPointData(const std::size_t index) const
{
    char* begin(m_mapping + getByteOffset(index));
    char* end(m_mapping + getByteOffset(index + 1));

    return std::vector<char>(begin, end);
}

void Chunk::sync()
{
    if (m_mapping && msync(m_mapping, m_size, MS_ASYNC) == -1)
    {
        throw std::runtime_error("Couldn't sync mapping");
    }
}

std::size_t Chunk::getByteOffset(std::size_t index) const
{
    return (index - m_begin) * m_schema.pointSize();
}

LockedChunk::LockedChunk(const std::size_t begin)
    : m_begin(begin)
    , m_mutex()
    , m_chunk(0)
{ }

LockedChunk::~LockedChunk()
{
    if (live())
    {
        delete m_chunk.load();
    }
}

Chunk* LockedChunk::init(
        const Schema& schema,
        const std::string& path,
        const std::vector<char>& initData)
{
    if (!live())
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!live())
        {
            m_chunk.store(new Chunk(schema, path, m_begin, initData));
        }
    }

    return get();
}

Chunk* LockedChunk::awaken(
        const Schema& schema,
        const std::string& path,
        const std::size_t chunkSize)
{
    if (backed(path))
    {
        if (!live())
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!live())
            {
                m_chunk.store(new Chunk(schema, path, m_begin, chunkSize));
            }
        }
    }

    return get();
}

bool LockedChunk::backed(const std::string& path) const
{
    return fs::fileExists(getFilename(path, m_begin));
}

bool LockedChunk::live() const
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
    , m_ids()
    , m_pointsPerChunk(pointsPerChunk(depthBegin, dimensions))
    , m_chunks()
    , m_emptyChunk(makeEmptyChunk(schema, m_pointsPerChunk))
{
    const std::size_t chunks(numChunks(depthBegin, depthEnd, dimensions));

    for (std::size_t i(0); i < chunks; ++i)
    {
        m_chunks.emplace_back(
                std::unique_ptr<LockedChunk>(
                    new LockedChunk(indexBegin() + i * m_pointsPerChunk)));
    }
}

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_path(path)
    , m_ids()
    , m_pointsPerChunk(pointsPerChunk(depthBegin(), dimensions))
    , m_chunks()
    , m_emptyChunk(makeEmptyChunk(schema, m_pointsPerChunk))
{
    const std::size_t chunks(numChunks(depthBegin(), depthEnd(), dimensions));

    for (std::size_t i(0); i < chunks; ++i)
    {
        m_chunks.emplace_back(
                std::unique_ptr<LockedChunk>(
                    new LockedChunk(indexBegin() + i * m_pointsPerChunk)));
    }
}

bool DiskBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    LockedChunk& lockedChunk(getLockedChunk(roller.pos()));

    if (!lockedChunk.live())
    {
        // First point position belonging to this chunk.
        lockedChunk.init(schema(), m_path, m_emptyChunk);

        std::lock_guard<std::mutex> lock(m_mutex);
        m_ids.insert(lockedChunk.id());
    }

    if (Chunk* chunk = lockedChunk.get())
    {
        return chunk->addPoint(roller, toAddPtr);
    }
    else
    {
        throw std::runtime_error("Couldn't create Chunk");
    }
}

bool DiskBranch::hasPoint(std::size_t index)
{
    bool result(false);

    const std::size_t chunkSize(m_pointsPerChunk * schema().pointSize());
    LockedChunk& lockedChunk(getLockedChunk(index));

    if (lockedChunk.backed(m_path))
    {
        if (Chunk* chunk = lockedChunk.awaken(schema(), m_path, chunkSize))
        {
            result = chunk->hasPoint(index);
        }
    }

    return result;
}

Point DiskBranch::getPoint(std::size_t index)
{
    Point point(INFINITY, INFINITY);

    const std::size_t chunkSize(m_pointsPerChunk * schema().pointSize());
    LockedChunk& lockedChunk(getLockedChunk(index));

    if (lockedChunk.backed(m_path))
    {
        if (Chunk* chunk = lockedChunk.awaken(schema(), m_path, chunkSize))
        {
            point = chunk->getPoint(index);
        }
    }

    return point;
}

std::vector<char> DiskBranch::getPointData(std::size_t index)
{
    return std::vector<char>();
}

LockedChunk& DiskBranch::getLockedChunk(const std::size_t index)
{
    return *m_chunks[getChunkIndex(index)].get();
}

std::size_t DiskBranch::getChunkIndex(std::size_t index) const
{
    assert(index >= indexBegin() && index < indexEnd());

    return (index - indexBegin()) / m_pointsPerChunk;
}

void DiskBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    for (const auto& id : m_ids)
    {
        meta["ids"].append(static_cast<Json::UInt64>(id));
    }
}

} // namespace entwine

