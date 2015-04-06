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
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/file-descriptor.hpp>
#include <entwine/util/platform.hpp>
#include <entwine/util/point-mapper.hpp>

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

    std::vector<char> makeEmptyChunk(const Schema& schema)
    {
        SimplePointTable table(schema);
        pdal::PointView view(table);

        // We require that the disk branch is no shallower than depth 6, so
        // there will always be a multiple of 4096 points per chunk.
        for (std::size_t i(0); i < 4096; ++i)
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
}

ChunkManager::ChunkManager(
        const std::string& path,
        const Schema& schema,
        const std::size_t begin,
        const std::size_t chunkSize)
    : m_filename(getFilename(path, begin))
    , m_schema(schema)
    , m_begin(begin)
    , m_chunkSize(chunkSize)
    , m_mutex()
    , m_mapper(0)
{ }

ChunkManager::~ChunkManager()
{
    if (live())
    {
        delete m_mapper.load();
    }
}

fs::PointMapper* ChunkManager::getMapper()
{
    if (!live())
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!live() && fs::fileExists(m_filename))
        {
            m_mapper.store(
                    new fs::PointMapper(
                        m_schema,
                        m_filename,
                        m_chunkSize,
                        m_begin,
                        m_chunkSize / m_schema.pointSize()));
        }
    }

    return m_mapper.load();
}

bool ChunkManager::create(const std::vector<char>& initData)
{
    bool created(false);

    if (!live())
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!live() && !fs::fileExists(m_filename))
        {
            const std::size_t dataSize(initData.size());
            assert(m_chunkSize % dataSize == 0);

            std::ofstream writer(m_filename, fs::binaryTruncMode);

            if (!writer.good())
            {
                throw std::runtime_error("Couldn't open chunk file");
            }

            for (std::size_t i(0); i < m_chunkSize / dataSize; ++i)
            {
                writer.write(initData.data(), dataSize);
            }

            created = true;
        }
    }

    return created;
}

bool ChunkManager::live() const
{
    return m_mapper.load() != 0;
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
    , m_mappers()
    , m_emptyChunk(makeEmptyChunk(schema))
{
    init();
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
    , m_mappers()
    , m_emptyChunk(makeEmptyChunk(schema))
{
    init();
}

void DiskBranch::init()
{
    if (depthBegin() < 6)
    {
        throw std::runtime_error("DiskBranch needs depthBegin >= 6");
    }

    const std::size_t mappers(numChunks(depthBegin(), depthEnd(), dimensions()));

    const std::size_t chunkSize(m_pointsPerChunk * schema().pointSize());

    for (std::size_t i(0); i < mappers; ++i)
    {
        m_mappers.emplace_back(
                std::unique_ptr<ChunkManager>(
                    new ChunkManager(
                        m_path,
                        schema(),
                        indexBegin() + i * m_pointsPerChunk,
                        chunkSize)));
    }
}

bool DiskBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    ChunkManager& mapperManager(getChunkManager(roller.pos()));

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        return mapper->addPoint(toAddPtr, roller);
    }
    else
    {
        throw std::runtime_error("Chunk wasn't created");
    }
}

bool DiskBranch::hasPoint(std::size_t index)
{
    bool result(false);

    ChunkManager& mapperManager(getChunkManager(index));

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        return mapper->hasPoint(index);
    }

    return result;
}

Point DiskBranch::getPoint(std::size_t index)
{
    Point point(INFINITY, INFINITY);

    ChunkManager& mapperManager(getChunkManager(index));

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        point = mapper->getPoint(index);
    }

    return point;
}

std::vector<char> DiskBranch::getPointData(std::size_t index)
{
    std::vector<char> data;

    ChunkManager& mapperManager(getChunkManager(index));

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        data = mapper->getPointData(index);
    }

    return data;
}

void DiskBranch::grow(Clipper* clipper, std::size_t index)
{
    ChunkManager& mapperManager(getChunkManager(index));

    if (mapperManager.create(m_emptyChunk))
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_ids.insert(mapperManager.id());
    }

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        mapper->grow(clipper, index);
    }
}

void DiskBranch::clip(Clipper* clipper, std::size_t index)
{
    ChunkManager& mapperManager(getChunkManager(index));

    if (fs::PointMapper* mapper = mapperManager.getMapper())
    {
        mapper->clip(clipper, index);
    }
}

ChunkManager& DiskBranch::getChunkManager(const std::size_t index)
{
    return *m_mappers[getChunkIndex(index)].get();
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

