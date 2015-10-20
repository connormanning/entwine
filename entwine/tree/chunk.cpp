/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/chunk.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

namespace
{
    std::atomic_size_t chunkMem(0);
    std::atomic_size_t chunkCnt(0);

    const std::size_t putRetries(20);

    const std::string tubeIdDim("TubeId");

    void ensurePut(
            const arbiter::Endpoint& endpoint,
            const std::string& path,
            const std::vector<char>& data)
    {
        bool done(false);
        std::size_t retries(0);

        while (!done)
        {
            try
            {
                endpoint.putSubpath(path, data);
                done = true;
            }
            catch (...)
            {
                if (++retries < putRetries)
                {
                    std::this_thread::sleep_for(std::chrono::seconds(retries));

                    std::cout <<
                        "\tFailed PUT attempt " << retries << ": " <<
                        endpoint.fullPath(path) <<
                        std::endl;
                }
                else
                {
                    std::cout <<
                        "\tFailed to PUT data: persistent failure.\n" <<
                        "\tThis is a non-recoverable error - Exiting..." <<
                        std::endl;

                    exit(1);
                }
            }
        }
    }
}

Chunk::Chunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints,
        const std::size_t numPoints)
    : m_schema(schema)
    , m_bbox(bbox)
    , m_structure(structure)
    , m_pools(pools)
    , m_depth(depth)
    , m_zDepth(std::min(structure.sparseDepthBegin() + 1, depth))
    , m_id(id)
    , m_maxPoints(maxPoints)
    , m_numPoints(numPoints)
{
    chunkMem.fetch_add(m_numPoints * m_schema.pointSize());
    chunkCnt.fetch_add(1);
}

Chunk::~Chunk()
{
    chunkMem.fetch_sub(m_numPoints * m_schema.pointSize());
    chunkCnt.fetch_sub(1);
}

std::unique_ptr<Chunk> Chunk::create(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints,
        const bool contiguous)
{
    std::unique_ptr<Chunk> chunk;

    if (contiguous)
    {
        chunk.reset(
                new ContiguousChunk(
                    schema,
                    bbox,
                    structure,
                    pools,
                    depth,
                    id,
                    maxPoints));
    }
    else
    {
        chunk.reset(
                new SparseChunk(
                    schema,
                    bbox,
                    structure,
                    pools,
                    depth,
                    id,
                    maxPoints));
    }

    return chunk;
}

std::unique_ptr<Chunk> Chunk::create(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints,
        std::vector<char> data)
{
    std::unique_ptr<Chunk> chunk;

    const Tail tail(popTail(data));
    const std::size_t points(tail.numPoints);

    if (tail.type == Contiguous)
    {
        chunk.reset(
                new ContiguousChunk(
                    schema,
                    bbox,
                    structure,
                    pools,
                    depth,
                    id,
                    maxPoints,
                    data,
                    points));
    }
    else
    {
        chunk.reset(
                new SparseChunk(
                    schema,
                    bbox,
                    structure,
                    pools,
                    depth,
                    id,
                    maxPoints,
                    data,
                    points));
    }

    return chunk;
}

void Chunk::pushTail(std::vector<char>& data, const Chunk::Tail tail)
{
    data.insert(
            data.end(),
            reinterpret_cast<const char*>(&tail.numPoints),
            reinterpret_cast<const char*>(&tail.numPoints) + sizeof(uint64_t));

    data.push_back(tail.type);
}

Chunk::Tail Chunk::popTail(std::vector<char>& data)
{
    // Pop type.
    Chunk::Type type;

    if (!data.empty())
    {
        const char marker(data.back());
        data.pop_back();

        if (marker == Sparse) type = Sparse;
        else if (marker == Contiguous) type = Contiguous;
        else throw std::runtime_error("Invalid chunk type detected");
    }
    else
    {
        throw std::runtime_error("Invalid chunk data detected");
    }

    // Pop numPoints.
    uint64_t numPoints(0);
    const std::size_t size(sizeof(uint64_t));

    if (data.size() < size)
    {
        throw std::runtime_error("Invalid serialized sparse chunk");
    }

    std::copy(
            data.data() + data.size() - size,
            data.data() + data.size(),
            reinterpret_cast<char*>(&numPoints));

    data.resize(data.size() - size);

    return Tail(numPoints, type);
}

std::size_t Chunk::getChunkMem() { return chunkMem.load(); }
std::size_t Chunk::getChunkCnt() { return chunkCnt.load(); }

std::size_t Chunk::normalize(const Id& rawIndex) const
{
    assert(rawIndex >= m_id);
    assert(rawIndex < endId());

    return (rawIndex - m_id).getSimple();
}

///////////////////////////////////////////////////////////////////////////////

SparseChunk::SparseChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints)
    , m_tubes()
    , m_mutex()
{ }

SparseChunk::SparseChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints,
        std::vector<char>& compressedData,
        const std::size_t numPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints, numPoints)
    , m_tubes()
    , m_mutex()
{
    // TODO This is direct copy/paste from the ContiguousChunk ctor.
    const std::size_t pointSize(m_schema.pointSize());

    std::unique_ptr<std::vector<char>> data(
            Compression::decompress(
                compressedData,
                m_schema,
                m_numPoints * pointSize));

    SinglePointTable table(m_schema);
    LinkingPointView view(table);

    std::size_t tube(0);
    std::size_t tick(0);

    if (m_numPoints * pointSize != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - sparse chunk");
    }

    PooledDataStack dataStack(m_pools.dataPool().acquire(m_numPoints));
    PooledInfoStack infoStack(m_pools.infoPool().acquire(m_numPoints));

    const std::size_t ticks(std::sqrt(m_maxPoints));

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledDataNode dataNode(dataStack.popOne());
        PooledInfoNode infoNode(infoStack.popOne());

        assert(dataNode.get());
        assert(infoNode.get());

        char* pos(data->data() + i * pointSize);
        std::copy(pos, pos + pointSize, dataNode->val());
        table.setData(pos);

        infoNode->construct(
                Point(
                    view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                std::move(dataNode));

        const Point& point(infoNode->val().point());

        tube = Tube::calcTube(point, m_bbox, ticks);
        tick = Tube::calcTick(point, m_bbox, m_zDepth);

        m_tubes[tube].addCell(tick, std::move(infoNode));
    }
}

Cell& SparseChunk::getCell(const Climber& climber)
{
    std::size_t norm(normalize(climber.index()));

    std::unique_lock<std::mutex> lock(m_mutex);
    Tube& tube(m_tubes[norm]);
    lock.unlock();

    std::pair<bool, Cell&> result(tube.getCell(climber.tick()));
    if (result.first)
    {
        chunkMem.fetch_add(m_schema.pointSize());
        ++m_numPoints;
    }
    return result.second;
}

void SparseChunk::save(arbiter::Endpoint& endpoint)
{
    // TODO Nearly direct copy/paste from ContiguousChunk::save.
    Compressor compressor(m_schema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (const auto& pair : m_tubes)
    {
        pair.second.save(m_schema, pair.first, data, dataStack, infoStack);

        if (data.size())
        {
            compressor.push(data.data(), data.size());
            data.clear();
        }
    }

    std::vector<char> compressed(compressor.data());
    dataStack.reset();
    infoStack.reset();
    pushTail(compressed, Tail(m_numPoints, Sparse));
    ensurePut(endpoint, m_id.str(), compressed);
}

///////////////////////////////////////////////////////////////////////////////

ContiguousChunk::ContiguousChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints)
    , m_tubes(maxPoints)
{ }

ContiguousChunk::ContiguousChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const std::size_t maxPoints,
        std::vector<char>& compressedData,
        const std::size_t numPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints, numPoints)
    , m_tubes(maxPoints)
{
    const std::size_t pointSize(m_schema.pointSize());

    std::unique_ptr<std::vector<char>> data(
            Compression::decompress(
                compressedData,
                m_schema,
                m_numPoints * pointSize));

    SinglePointTable table(m_schema);
    LinkingPointView view(table);

    std::size_t tube(0);
    std::size_t tick(0);

    if (m_numPoints * pointSize != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - contiguous chunk");
    }

    PooledDataStack dataStack(m_pools.dataPool().acquire(m_numPoints));
    PooledInfoStack infoStack(m_pools.infoPool().acquire(m_numPoints));

    std::size_t ticks(m_zDepth ? std::sqrt(m_maxPoints) : 0);

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledDataNode dataNode(dataStack.popOne());
        PooledInfoNode infoNode(infoStack.popOne());

        assert(dataNode.get());
        assert(infoNode.get());

        char* pos(data->data() + i * pointSize);
        std::copy(pos, pos + pointSize, dataNode->val());
        table.setData(pos);

        infoNode->construct(
                Point(
                    view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                std::move(dataNode));

        const std::size_t depth(
                m_zDepth ?
                    m_zDepth :
                    ChunkInfo::calcDepth(m_structure.factor(), m_id + i));

        const Point& point(infoNode->val().point());

        tick = Tube::calcTick(point, m_bbox, depth);

        if (m_zDepth)
        {
            tube = Tube::calcTube(point, m_bbox, ticks);
            tick = Tube::calcTick(point, m_bbox, m_depth);
        }
        else
        {
            tick = Tube::calcTick(
                    point,
                    m_bbox,
                    ChunkInfo::calcDepth(m_structure.factor(), m_id + i));

            const std::size_t depth(
                    ChunkInfo::calcDepth(
                        m_structure.factor(),
                        m_id + i));

            const std::size_t pointsAtDepth(
                    ChunkInfo::pointsAtDepth(
                        m_structure.dimensions(),
                        depth).getSimple());

            const std::size_t levelIndex(
                    ChunkInfo::calcLevelIndex(
                        m_structure.dimensions(),
                        depth).getSimple());

            ticks = std::sqrt(pointsAtDepth);

            tube =
                Tube::calcTube(point, m_bbox, ticks) +
                levelIndex -
                m_id.getSimple();
        }

        m_tubes.at(tube).addCell(tick, std::move(infoNode));
    }
}

Cell& ContiguousChunk::getCell(const Climber& climber)
{
    Tube& tube(m_tubes.at(normalize(climber.index())));

    std::pair<bool, Cell&> result(tube.getCell(climber.tick()));
    if (result.first)
    {
        chunkMem.fetch_add(m_schema.pointSize());
        ++m_numPoints;
    }
    return result.second;
}

void ContiguousChunk::save(
        arbiter::Endpoint& endpoint,
        const std::string postfix)
{
    Compressor compressor(m_schema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        m_tubes[i].save(m_schema, i, data, dataStack, infoStack);

        if (data.size())
        {
            compressor.push(data.data(), data.size());
            data.clear();
        }
    }

    std::vector<char> compressed(compressor.data());
    dataStack.reset();
    infoStack.reset();
    pushTail(compressed, Tail(m_numPoints, Contiguous));
    ensurePut(endpoint, m_id.str() + postfix, compressed);
}

void ContiguousChunk::save(arbiter::Endpoint& endpoint)
{
    save(endpoint, "");
}

void ContiguousChunk::merge(ContiguousChunk& other)
{
    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        Tube& ours(m_tubes.at(i));
        const Tube& theirs(other.m_tubes.at(i));

        if (!ours.empty() && !theirs.empty())
        {
            throw std::runtime_error("Tube mismatch");
        }

        if (!theirs.empty())
        {
            ours = theirs;
        }
    }
}

} // namespace entwine

