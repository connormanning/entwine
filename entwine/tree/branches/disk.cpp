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
#include <entwine/util/pool.hpp>

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
            view.setField(pdal::Dimension::Id::X, i, 0);//INFINITY);
            view.setField(pdal::Dimension::Id::Y, i, 0);//INFINITY);
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
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(source, schema, dimensions, depthBegin, depthEnd)
    , m_path(source.path())
    , m_ids()
    , m_pointsPerChunk(pointsPerChunk(depthBegin, dimensions))
    , m_chunkManagers()
    , m_emptyChunk(makeEmptyChunk(schema))
{
    initChunkManagers();
}

DiskBranch::DiskBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(source, schema, dimensions, meta)
    , m_path(source.path())
    , m_ids()
    , m_pointsPerChunk(pointsPerChunk(depthBegin(), dimensions))
    , m_chunkManagers()
    , m_emptyChunk(makeEmptyChunk(schema))
{
    initChunkManagers();

    const Json::Value& metaIds(meta["ids"]);

    if (metaIds.isArray())
    {
        for (Json::ArrayIndex i(0); i < metaIds.size(); ++i)
        {
            m_ids.insert(metaIds[i].asUInt64());
        }
    }
}

void DiskBranch::initChunkManagers()
{
    if (depthBegin() < 6)
    {
        throw std::runtime_error("DiskBranch needs depthBegin >= 6");
    }

    const std::size_t mappers(
            numChunks(depthBegin(), depthEnd(), dimensions()));

    const std::size_t chunkSize(m_pointsPerChunk * schema().pointSize());

    for (std::size_t i(0); i < mappers; ++i)
    {
        m_chunkManagers.emplace_back(
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
    ChunkManager& chunkManager(getChunkManager(roller.pos()));

    if (fs::PointMapper* mapper = chunkManager.getMapper())
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

    ChunkManager& chunkManager(getChunkManager(index));

    if (fs::PointMapper* mapper = chunkManager.getMapper())
    {
        return mapper->hasPoint(index);
    }

    return result;
}

Point DiskBranch::getPoint(std::size_t index)
{
    Point point;

    ChunkManager& chunkManager(getChunkManager(index));

    if (fs::PointMapper* mapper = chunkManager.getMapper())
    {
        point = mapper->getPoint(index);
    }

    return point;
}

std::vector<char> DiskBranch::getPointData(std::size_t index)
{
    std::vector<char> data;

    ChunkManager& chunkManager(getChunkManager(index));

    if (fs::PointMapper* mapper = chunkManager.getMapper())
    {
        data = mapper->getPointData(index);
    }

    return data;
}

void DiskBranch::grow(Clipper* clipper, std::size_t index)
{
    ChunkManager& chunkManager(getChunkManager(index));
    chunkManager.create(m_emptyChunk);

    if (fs::PointMapper* mapper = chunkManager.getMapper())
    {
        mapper->grow(clipper, index);
    }
}

void DiskBranch::clip(Clipper* clipper, std::size_t index)
{
    ChunkManager& chunkManager(getChunkManager(index));

    if (fs::PointMapper* mapper = chunkManager.getMapper())
    {
        mapper->clip(clipper, index);
    }
}

ChunkManager& DiskBranch::getChunkManager(const std::size_t index)
{
    assert(index >= indexBegin() && index < indexEnd());
    const std::size_t normalized((index - indexBegin()) / m_pointsPerChunk);
    return *m_chunkManagers[normalized].get();
}

void DiskBranch::saveImpl(Json::Value& meta)
{
    /*
    for (std::size_t i(0); i < m_chunkManagers.size(); ++i)
    {
        ChunkManager& chunkManager(*m_chunkManagers[i].get());

        if (fs::PointMapper* mapper = chunkManager.getMapper())
        {
            std::vector<std::size_t> mapperIds(mapper->ids());
            m_ids.insert(mapperIds.begin(), mapperIds.end());
        }
    }

    for (const auto id : m_ids)
    {
        meta["ids"].append(static_cast<Json::UInt64>(id));
    }
    */
}

void DiskBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t chunkSize)
{
    /*
    for (auto& c : m_chunkManagers)
    {
        ChunkManager& chunkManager(*c.get());

        if (fs::PointMapper* mapper = chunkManager.getMapper())
        {
            mapper->finalize(output, pool, ids, start, chunkSize);
        }
    }

    pool.join();
    */
}

} // namespace entwine

