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
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

namespace
{
    std::atomic_size_t chunkMem(0);
    std::atomic_size_t chunkCnt(0);

    const std::string tubeIdDim("TubeId");

    Schema makeCelled(const Schema& in)
    {
        DimList dims;
        dims.push_back(DimInfo(tubeIdDim, "unsigned", 8));
        dims.insert(dims.end(), in.dims().begin(), in.dims().end());
        return Schema(dims);
    }
}

Chunk::Chunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        const std::size_t numPoints)
    : m_schema(schema)
    , m_bbox(bbox)
    , m_structure(structure)
    , m_pools(pools)
    , m_zDepth(std::min(Tube::maxTickDepth(), depth))
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
        const Id& maxPoints,
        const bool contiguous)
{
    std::unique_ptr<Chunk> chunk;

    if (contiguous)
    {
        if (depth)
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
                    new BaseChunk(
                        schema,
                        bbox,
                        structure,
                        pools,
                        id,
                        maxPoints));
        }
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
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> data)
{
    std::unique_ptr<Chunk> chunk;

    const Tail tail(popTail(*data));
    const std::size_t points(tail.numPoints);

    if (tail.type == Contiguous)
    {
        if (depth)
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
                        std::move(data),
                        points));
        }
        else
        {
            chunk.reset(
                    new BaseChunk(
                        schema,
                        bbox,
                        structure,
                        pools,
                        id,
                        maxPoints,
                        std::move(data),
                        points));
        }
    }
    else if (tail.type == Sparse)
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
                    std::move(data),
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
        const int marker(data.back());
        data.pop_back();

        if (marker == Sparse) type = Sparse;
        else if (marker == Contiguous) type = Contiguous;
        else return Tail(0, Invalid);
    }
    else
    {
        return Tail(0, Invalid);
    }

    // Pop numPoints.
    uint64_t numPoints(0);
    const std::size_t size(sizeof(uint64_t));

    if (data.size() < size) return Tail(0, Invalid);

    std::copy(
            data.data() + data.size() - size,
            data.data() + data.size(),
            reinterpret_cast<char*>(&numPoints));

    data.resize(data.size() - size);

    return Tail(numPoints, type);
}

std::size_t Chunk::getChunkMem() { return chunkMem.load(); }
std::size_t Chunk::getChunkCnt() { return chunkCnt.load(); }

///////////////////////////////////////////////////////////////////////////////

SparseChunk::SparseChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
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
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints, numPoints)
    , m_tubes()
    , m_mutex()
{
    // TODO This is direct copy/paste from the ContiguousChunk ctor.
    PooledInfoStack infoStack(
            Compression::decompress(*compressedData, m_numPoints, m_pools));

    Id tube(0);
    std::size_t tick(0);

    if (m_numPoints != infoStack.size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - sparse chunk");
    }

    // TODO Very dense data might throw here.  See comment in calcTube for
    // discussion on what needs to change if that happens.
    const std::size_t ticks(sqrt(m_maxPoints).getSimple());
    const bool tubular(m_structure.tubular());

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledInfoNode infoNode(infoStack.popOne());
        const Point& point(infoNode->val().point());

        tube = Tube::calcTube(point, m_bbox, ticks);
        if (tubular) tick = Tube::calcTick(point, m_bbox, m_zDepth);

        m_tubes[tube].addCell(tick, std::move(infoNode));
    }
}

Cell& SparseChunk::getCell(const Climber& climber)
{
    const Id norm(normalize(climber.index()));

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
        pair.second.save(m_schema, data, dataStack, infoStack);

        if (data.size())
        {
            compressor.push(data.data(), data.size());
            data.clear();
        }
    }

    std::unique_ptr<std::vector<char>> compressed(compressor.data());
    dataStack.reset();
    infoStack.reset();
    pushTail(*compressed, Tail(m_numPoints, Sparse));
    Storage::ensurePut(endpoint, m_structure.maybePrefix(m_id), *compressed);
}

///////////////////////////////////////////////////////////////////////////////

ContiguousChunk::ContiguousChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{ }

ContiguousChunk::ContiguousChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : Chunk(schema, bbox, structure, pools, depth, id, maxPoints, numPoints)
    , m_tubes(maxPoints.getSimple())
{
    PooledInfoStack infoStack(
            Compression::decompress(*compressedData, m_numPoints, m_pools));

    std::size_t tube(0);
    std::size_t tick(0);

    if (m_numPoints != infoStack.size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - contiguous chunk");
    }

    const std::size_t ticks(std::sqrt(m_maxPoints.getSimple()));
    const bool tubular(m_structure.tubular());

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        PooledInfoNode infoNode(infoStack.popOne());
        const Point& point(infoNode->val().point());

        tube = Tube::calcTube(point, m_bbox, ticks).getSimple();
        if (tubular) tick = Tube::calcTick(point, m_bbox, m_zDepth);

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

void ContiguousChunk::save(arbiter::Endpoint& endpoint)
{
    Compressor compressor(m_schema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        m_tubes[i].save(m_schema, data, dataStack, infoStack);

        if (data.size())
        {
            compressor.push(data.data(), data.size());
            data.clear();
        }
    }

    std::unique_ptr<std::vector<char>> compressed(compressor.data());
    dataStack.reset();
    infoStack.reset();
    pushTail(*compressed, Tail(m_numPoints, Contiguous));
    Storage::ensurePut(endpoint, m_structure.maybePrefix(m_id), *compressed);
}

///////////////////////////////////////////////////////////////////////////////

BaseChunk::BaseChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const Id& id,
        const Id& maxPoints)
    : ContiguousChunk(schema, bbox, structure, pools, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_schema))
{ }

BaseChunk::BaseChunk(
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pools,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : ContiguousChunk(schema, bbox, structure, pools, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_schema))
{
    m_numPoints = numPoints;

    std::unique_ptr<std::vector<char>> data(
        Compression::decompress(*compressedData, m_celledSchema, m_numPoints));

    const char* pos(data->data());

    if (m_numPoints * m_celledSchema.pointSize() != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - base chunk");
    }

    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const bool tubular(m_structure.tubular());
    const auto tubeId(m_celledSchema.pdalLayout().findDim(tubeIdDim));

    // Skip tube IDs.
    const std::size_t dataOffset(sizeof(uint64_t));

    BinaryPointTable table(m_celledSchema);
    pdal::PointRef pointRef(table, 0);

    PooledInfoStack infoStack(m_pools.infoPool().acquire(m_numPoints));
    PooledDataStack dataStack(m_pools.dataPool().acquire(m_numPoints));

    std::size_t tube(0);
    std::size_t curDepth(0);
    std::size_t tick(0);

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        table.setPoint(pos);

        PooledInfoNode info(infoStack.popOne());
        info->construct(
                Point(
                    pointRef.getFieldAs<double>(pdal::Dimension::Id::X),
                    pointRef.getFieldAs<double>(pdal::Dimension::Id::Y),
                    pointRef.getFieldAs<double>(pdal::Dimension::Id::Z)),
                dataStack.popOne());

        std::copy(pos + dataOffset, pos + celledPointSize, info->val().data());

        if (tubular)
        {
            tube = pointRef.getFieldAs<uint64_t>(tubeId);
            curDepth = ChunkInfo::calcDepth(m_structure.factor(), m_id + tube);
            tick = Tube::calcTick(info->val().point(), m_bbox, curDepth);
        }

        m_tubes.at(tube).addCell(tick, std::move(info));

        pos += celledPointSize;
    }
}

void BaseChunk::save(arbiter::Endpoint& endpoint, std::string postfix)
{
    Compressor compressor(m_celledSchema);
    std::vector<char> data;

    PooledDataStack dataStack(m_pools.dataPool());
    PooledInfoStack infoStack(m_pools.infoPool());

    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        m_tubes[i].saveBase(m_celledSchema, i, data, dataStack, infoStack);

        if (data.size())
        {
            compressor.push(data.data(), data.size());
            data.clear();
        }
    }

    std::unique_ptr<std::vector<char>> compressed(compressor.data());
    dataStack.reset();
    infoStack.reset();
    pushTail(*compressed, Tail(m_numPoints, Contiguous));
    Storage::ensurePut(endpoint, m_id.str() + postfix, *compressed);
}

void BaseChunk::save(arbiter::Endpoint& endpoint)
{
    save(endpoint, "");
}

void BaseChunk::merge(BaseChunk& other)
{
    m_numPoints += other.m_numPoints;

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

