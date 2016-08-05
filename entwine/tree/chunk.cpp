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

#include <atomic>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    std::atomic_size_t chunkCount(0);
    const std::string tubeIdDim("TubeId");
}

std::size_t Chunk::count() { return chunkCount; }

Chunk::Chunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : m_builder(builder)
    , m_metadata(m_builder.metadata())
    , m_pointPool(m_builder.pointPool())
    , m_depth(depth)
    , m_zDepth(std::min(Tube::maxTickDepth(), depth))
    , m_id(id)
    , m_maxPoints(maxPoints)
    , m_data()
{
    ++chunkCount;
}

void Chunk::populate(Cell::PooledStack cells)
{
    Climber climber(m_metadata);

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());

        climber.reset();
        climber.magnifyTo(cell->point(), m_depth);

        insert(climber, cell);
    }
}

void Chunk::collect(ChunkType type)
{
    assert(!m_data);

    Cell::PooledStack cellStack(acquire());
    Data::PooledStack dataStack(m_pointPool.dataPool());

    for (Cell& cell : cellStack) dataStack.push(cell.acquire());

    cellStack.reset();

    m_data = m_metadata.format().pack(std::move(dataStack), type);
}

Chunk::~Chunk()
{
    if (m_data)
    {
        const std::string path(
                m_metadata.structure().maybePrefix(m_id) +
                m_metadata.postfix(true));

        Storage::ensurePut(m_builder.outEndpoint(), path, *m_data);
    }

    --chunkCount;
}

std::unique_ptr<Chunk> Chunk::create(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
{
    if (id < builder.metadata().structure().mappedIndexBegin())
    {
        if (depth)
        {
            return makeUnique<ContiguousChunk>(builder, depth, id, maxPoints);
        }
        else
        {
            return makeUnique<BaseChunk>(builder, id, maxPoints);
        }
    }
    else
    {
        return makeUnique<SparseChunk>(builder, depth, id, maxPoints);
    }
}

std::unique_ptr<Chunk> Chunk::create(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> data)
{
    Unpacker unpacker(builder.metadata().format().unpack(std::move(data)));

    if (depth)
    {
        if (unpacker.chunkType() == ChunkType::Contiguous)
        {
            return makeUnique<ContiguousChunk>(
                    builder,
                    depth,
                    id,
                    maxPoints,
                    unpacker.acquireCells(builder.pointPool()));
        }
        else if (unpacker.chunkType() == ChunkType::Sparse)
        {
            return makeUnique<SparseChunk>(
                    builder,
                    depth,
                    id,
                    maxPoints,
                    unpacker.acquireCells(builder.pointPool()));
        }
    }
    else
    {
        return makeUnique<BaseChunk>(
                builder,
                id,
                maxPoints,
                std::move(unpacker));
    }

    return std::unique_ptr<Chunk>();
}

///////////////////////////////////////////////////////////////////////////////

SparseChunk::SparseChunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes()
    , m_mutex()
{ }

SparseChunk::SparseChunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        Cell::PooledStack cells)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes()
    , m_mutex()
{
    populate(std::move(cells));
}

SparseChunk::~SparseChunk()
{
    collect(ChunkType::Sparse);
}

Cell::PooledStack SparseChunk::acquire()
{
    Cell::PooledStack cells(m_pointPool.cellPool());

    for (auto& outer : m_tubes)
    {
        Tube& tube(outer.second);

        for (auto& inner : tube)
        {
            cells.push(std::move(inner.second));
        }
    }

    return cells;
}

///////////////////////////////////////////////////////////////////////////////

ContiguousChunk::ContiguousChunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{ }

ContiguousChunk::ContiguousChunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        Cell::PooledStack cells)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{
    populate(std::move(cells));
}

ContiguousChunk::~ContiguousChunk()
{
    // Don't run collect if we are a BaseChunk.
    if (m_id != m_metadata.structure().baseIndexBegin())
    {
        collect(ChunkType::Contiguous);
    }
}

Cell::PooledStack ContiguousChunk::acquire()
{
    Cell::PooledStack cells(m_pointPool.cellPool());

    for (Tube& tube : m_tubes)
    {
        for (auto& inner : tube) cells.push(std::move(inner.second));
    }

    return cells;
}

///////////////////////////////////////////////////////////////////////////////

BaseChunk::BaseChunk(
        const Builder& builder,
        const Id& id,
        const Id& maxPoints)
    : ContiguousChunk(builder, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_metadata.schema()))
{ }

BaseChunk::BaseChunk(
        const Builder& builder,
        const Id& id,
        const Id& maxPoints,
        Unpacker unpacker)
    : ContiguousChunk(builder, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_metadata.schema()))
{
    auto data(unpacker.acquireRawBytes());
    const std::size_t numPoints(unpacker.numPoints());

    if (m_metadata.format().compress())
    {
        data = Compression::decompress(*data, m_celledSchema, numPoints);
    }

    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const auto tubeId(m_celledSchema.getId(tubeIdDim));

    // Skip tube IDs.
    const std::size_t dataOffset(sizeof(uint64_t));

    BinaryPointTable table(m_celledSchema);
    pdal::PointRef pointRef(table, 0);

    Cell::PooledStack cellStack(m_pointPool.cellPool().acquire(numPoints));
    Data::PooledStack dataStack(m_pointPool.dataPool().acquire(numPoints));

    const std::size_t factor(m_metadata.structure().factor());

    Climber climber(m_metadata);

    const char* pos(data->data());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        table.setPoint(pos);

        Data::PooledNode data(dataStack.popOne());
        std::copy(pos + dataOffset, pos + celledPointSize, *data);

        Cell::PooledNode cell(cellStack.popOne());
        cell->set(pointRef, std::move(data));

        const std::size_t tube(pointRef.getFieldAs<uint64_t>(tubeId));
        const std::size_t curDepth(ChunkInfo::calcDepth(factor, m_id + tube));

        climber.reset();
        climber.magnifyTo(cell->point(), curDepth);

        if (tube != normalize(climber.index()))
        {
            throw std::runtime_error("Bad serialized base tube");
        }

        insert(climber, cell);

        pos += celledPointSize;
    }
}

void BaseChunk::save(const arbiter::Endpoint& endpoint)
{
    Data::PooledStack dataStack(m_pointPool.dataPool());
    Cell::PooledStack cellStack(m_pointPool.cellPool());

    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const std::size_t nativePointSize(m_metadata.schema().pointSize());

    std::vector<char> point(celledPointSize);

    const std::size_t tubeIdSize(sizeof(uint64_t));

    uint64_t i(0);
    const char* iPos(reinterpret_cast<char*>(&i));
    const char* iEnd(iPos + tubeIdSize);

    const bool compress(m_metadata.format().compress());
    std::unique_ptr<Compressor> compressor(
            compress ? makeUnique<Compressor>(m_celledSchema) : nullptr);

    std::unique_ptr<std::vector<char>> data(makeUnique<std::vector<char>>());

    for ( ; i < m_tubes.size(); ++i)
    {
        Tube& tube(m_tubes[i]);

        for (auto& inner : tube)
        {
            Cell::PooledNode& cell(inner.second);

            for (const char* d : *cell)
            {
                std::copy(iPos, iEnd, point.data());
                std::copy(d, d + nativePointSize, point.data() + tubeIdSize);

                if (compress)
                {
                    compressor->push(point.data(), point.size());
                }
                else
                {
                    data->insert(
                            data->end(),
                            point.data(),
                            point.data() + point.size());
                }
            }

            dataStack.push(cell->acquire());
            cellStack.push(std::move(cell));
        }
    }

    if (compress) data = compressor->data();

    // Since the base is serialized with a different schema, we'll compress it
    // on our own.
    Packer packer(
            m_metadata.format().tailFields(),
            *data,
            dataStack.size(),
            ChunkType::Contiguous);
    auto tail(packer.buildTail());
    data->insert(data->end(), tail.begin(), tail.end());

    // No prefixing on base.
    const std::string path(m_id.str() + m_metadata.postfix());

    Storage::ensurePut(endpoint, path, *data);

    assert(!m_data);    // Don't let the parent destructor serialize.
}

Schema BaseChunk::makeCelled(const Schema& in)
{
    DimList dims;
    dims.push_back(DimInfo(tubeIdDim, "unsigned", 8));
    dims.insert(dims.end(), in.dims().begin(), in.dims().end());
    return Schema(dims);
}

void BaseChunk::merge(BaseChunk& other)
{
    for (std::size_t i(0); i < m_tubes.size(); ++i)
    {
        Tube& ours(m_tubes.at(i));
        Tube& theirs(other.m_tubes.at(i));

        if (!ours.empty() && !theirs.empty())
        {
            throw std::runtime_error("Tube mismatch at " + std::to_string(i));
        }

        if (!theirs.empty())
        {
            ours.swap(std::move(theirs));
        }
    }
}

} // namespace entwine

