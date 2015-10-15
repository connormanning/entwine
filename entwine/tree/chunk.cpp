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
    : m_nativeSchema(schema)
    , m_celledSchema(makeCelled(schema))
    , m_bbox(bbox)
    , m_structure(structure)
    , m_pools(pools)
    , m_depth(depth)
    , m_id(id)
    , m_maxPoints(maxPoints)
    , m_numPoints(numPoints)
{
    chunkMem.fetch_add(m_numPoints * m_nativeSchema.pointSize());
    chunkCnt.fetch_add(1);
}

Chunk::~Chunk()
{
    chunkMem.fetch_sub(m_numPoints * m_nativeSchema.pointSize());
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

Schema Chunk::makeCelled(const Schema& schema)
{
    DimList dims;
    dims.push_back(DimInfo(tubeIdDim, "unsigned", 8));
    dims.insert(dims.end(), schema.dims().begin(), schema.dims().end());
    return Schema(dims);
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
    const std::size_t celledPointSize(m_celledSchema.pointSize());

    std::unique_ptr<std::vector<char>> data(
            Compression::decompress(
                compressedData,
                m_celledSchema,
                m_numPoints * celledPointSize));

    SinglePointTable table(m_celledSchema);
    LinkingPointView view(table);

    const pdal::Dimension::Id::Enum tubeId(
            m_celledSchema.pdalLayout().findDim(tubeIdDim));

    std::size_t tube(0);
    std::size_t tick(0);

    // Skip tube IDs.
    const std::size_t dataOffset(sizeof(uint64_t));

    if (m_numPoints * celledPointSize != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - sparse chunk");
    }

    PooledDataStack dataStack(m_pools.dataPool().acquire(m_numPoints));
    PooledInfoStack infoStack(m_pools.infoPool().acquire(m_numPoints));

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledDataNode dataNode(dataStack.popOne());
        PooledInfoNode infoNode(infoStack.popOne());

        assert(dataNode.get());
        assert(infoNode.get());

        char* pos(data->data() + i * celledPointSize);
        std::copy(pos + dataOffset, pos + celledPointSize, dataNode->val());
        table.setData(pos);

        infoNode->construct(
                Point(
                    view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                std::move(dataNode));

        tube = view.getFieldAs<uint64_t>(tubeId, 0);
        tick = Tube::calcTick(infoNode->val().point(), m_bbox, m_depth);

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
        chunkMem.fetch_add(m_nativeSchema.pointSize());
        ++m_numPoints;
    }
    return result.second;
}

void SparseChunk::save(arbiter::Endpoint& endpoint)
{
    // TODO Nearly direct copy/paste from ContiguousChunk::save.
    Compressor compressor(m_celledSchema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (const auto& pair : m_tubes)
    {
        pair.second.save(
                m_celledSchema, pair.first, data, dataStack, infoStack);

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
    const std::size_t celledPointSize(m_celledSchema.pointSize());

    std::unique_ptr<std::vector<char>> data(
            Compression::decompress(
                compressedData,
                m_celledSchema,
                m_numPoints * celledPointSize));

    SinglePointTable table(m_celledSchema);
    LinkingPointView view(table);

    const pdal::Dimension::Id::Enum tubeId(
            m_celledSchema.pdalLayout().findDim(tubeIdDim));

    std::size_t tube(0);
    std::size_t tick(0);

    // Skip tube IDs.
    const std::size_t dataOffset(sizeof(uint64_t));

    if (m_numPoints * celledPointSize != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - contiguous chunk");
    }

    PooledDataStack dataStack(m_pools.dataPool().acquire(m_numPoints));
    PooledInfoStack infoStack(m_pools.infoPool().acquire(m_numPoints));

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledDataNode dataNode(dataStack.popOne());
        PooledInfoNode infoNode(infoStack.popOne());

        assert(dataNode.get());
        assert(infoNode.get());

        char* pos(data->data() + i * celledPointSize);
        std::copy(pos + dataOffset, pos + celledPointSize, dataNode->val());
        table.setData(pos);

        infoNode->construct(
                Point(
                    view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                    view.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                std::move(dataNode));

        tube = view.getFieldAs<uint64_t>(tubeId, 0);

        const std::size_t depth(
                m_depth ?
                    m_depth :
                    ChunkInfo::calcDepth(m_structure.factor(), m_id + i));

        tick = Tube::calcTick(infoNode->val().point(), m_bbox, depth);

        m_tubes.at(tube).addCell(tick, std::move(infoNode));
    }
}

Cell& ContiguousChunk::getCell(const Climber& climber)
{
    Tube& tube(m_tubes.at(normalize(climber.index())));

    std::pair<bool, Cell&> result(tube.getCell(climber.tick()));
    if (result.first)
    {
        chunkMem.fetch_add(m_nativeSchema.pointSize());
        ++m_numPoints;
    }
    return result.second;
}

void ContiguousChunk::save(
        arbiter::Endpoint& endpoint,
        const std::string postfix)
{
    Compressor compressor(m_celledSchema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        m_tubes[i].save(m_celledSchema, i, data, dataStack, infoStack);

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

