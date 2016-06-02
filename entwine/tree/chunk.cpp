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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

namespace
{
    const std::string tubeIdDim("TubeId");
}

Chunk::Chunk(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : m_builder(builder)
    , m_metadata(m_builder.metadata())
    , m_pointPool(m_builder.pointPool())
    , m_bbox(bbox)
    , m_depth(depth)
    , m_zDepth(std::min(Tube::maxTickDepth(), depth))
    , m_id(id)
    , m_maxPoints(maxPoints)
    , m_data()
{ }

void Chunk::populate(
        const std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
{
    Cell::PooledStack cells(
            Compression::decompress(*compressedData, numPoints, m_pointPool));

    const Climber startClimber(m_metadata.bbox(), m_metadata.structure());
    Climber climber(startClimber);

    for (std::size_t i(0); i < numPoints; ++i)
    {
        Cell::PooledNode cell(cells.popOne());

        climber = startClimber;
        climber.magnifyTo(cell->point(), m_depth);

        insert(climber, cell);
    }
}

void Chunk::collect(Type type)
{
    assert(!m_data);

    Cell::PooledStack cellStack(acquire());
    Data::PooledStack dataStack(m_pointPool.dataPool());

    for (Cell& cell : cellStack) dataStack.push(cell.acquire());

    cellStack.reset();

    const std::size_t pointSize(m_metadata.schema().pointSize());
    Compressor compressor(m_metadata.schema(), dataStack.size());

    for (const char* pos : dataStack) compressor.push(pos, pointSize);

    m_data = compressor.data();
    pushTail(*m_data, Tail(dataStack.size(), type));
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
}

std::unique_ptr<Chunk> Chunk::create(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
{
    std::unique_ptr<Chunk> chunk;

    if (id < builder.metadata().structure().sparseIndexBegin())
    {
        if (depth)
        {
            chunk.reset(
                    new ContiguousChunk(builder, bbox, depth, id, maxPoints));
        }
        else
        {
            chunk.reset(new BaseChunk(builder, bbox, id, maxPoints));
        }
    }
    else
    {
        chunk.reset(new SparseChunk(builder, bbox, depth, id, maxPoints));
    }

    return chunk;
}

std::unique_ptr<Chunk> Chunk::create(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> data)
{
    std::unique_ptr<Chunk> chunk;

    const Tail tail(popTail(*data));
    const std::size_t numPoints(tail.numPoints);

    if (tail.type == Type::Contiguous)
    {
        if (depth)
        {
            chunk.reset(
                    new ContiguousChunk(
                        builder,
                        bbox,
                        depth,
                        id,
                        maxPoints,
                        std::move(data),
                        numPoints));
        }
        else
        {
            chunk.reset(
                    new BaseChunk(
                        builder,
                        bbox,
                        id,
                        maxPoints,
                        std::move(data),
                        numPoints));
        }
    }
    else if (tail.type == Type::Sparse)
    {
        chunk.reset(
                new SparseChunk(
                    builder,
                    bbox,
                    depth,
                    id,
                    maxPoints,
                    std::move(data),
                    numPoints));
    }

    return chunk;
}

void Chunk::pushTail(std::vector<char>& data, const Chunk::Tail tail)
{
    data.insert(
            data.end(),
            reinterpret_cast<const char*>(&tail.numPoints),
            reinterpret_cast<const char*>(&tail.numPoints) + sizeof(uint64_t));

    data.push_back(static_cast<char>(tail.type));
}

Chunk::Tail Chunk::popTail(std::vector<char>& data)
{
    // Pop type.
    Type type(Type::Invalid);

    if (!data.empty())
    {
        type = static_cast<Type>(data.back());
        data.pop_back();
    }

    if (type != Type::Sparse && type != Type::Contiguous)
    {
        return Tail(0, Type::Invalid);
    }

    // Pop numPoints.
    uint64_t numPoints(0);
    const std::size_t size(sizeof(uint64_t));

    if (data.size() < size) return Tail(0, Type::Invalid);

    std::copy(
            data.data() + data.size() - size,
            data.data() + data.size(),
            reinterpret_cast<char*>(&numPoints));

    data.resize(data.size() - size);

    return Tail(numPoints, type);
}

///////////////////////////////////////////////////////////////////////////////

SparseChunk::SparseChunk(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : Chunk(builder, bbox, depth, id, maxPoints)
    , m_tubes()
    , m_mutex()
{ }

SparseChunk::SparseChunk(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : Chunk(builder, bbox, depth, id, maxPoints)
    , m_tubes()
    , m_mutex()
{
    populate(std::move(compressedData), numPoints);
}

SparseChunk::~SparseChunk()
{
    collect(Type::Sparse);
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
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : Chunk(builder, bbox, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{ }

ContiguousChunk::ContiguousChunk(
        const Builder& builder,
        const BBox& bbox,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : Chunk(builder, bbox, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{
    populate(std::move(compressedData), numPoints);
}

ContiguousChunk::~ContiguousChunk()
{
    // Don't run collect if we are a BaseChunk, in which case m_data will
    // already be populated.
    if (!m_data) collect(Type::Contiguous);
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
        const BBox& bbox,
        const Id& id,
        const Id& maxPoints)
    : ContiguousChunk(builder, bbox, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_metadata.schema()))
{ }

BaseChunk::BaseChunk(
        const Builder& builder,
        const BBox& bbox,
        const Id& id,
        const Id& maxPoints,
        std::unique_ptr<std::vector<char>> compressedData,
        const std::size_t numPoints)
    : ContiguousChunk(builder, bbox, 0, id, maxPoints)
    , m_celledSchema(makeCelled(m_metadata.schema()))
{
    std::cout << "Waking up base" << std::endl;

    std::unique_ptr<std::vector<char>> data(
        Compression::decompress(*compressedData, m_celledSchema, numPoints));

    if (numPoints * m_celledSchema.pointSize() != data->size())
    {
        // TODO Non-recoverable.  Exit?
        throw std::runtime_error("Bad numPoints detected - base chunk");
    }

    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const auto tubeId(m_celledSchema.pdalLayout().findDim(tubeIdDim));

    // Skip tube IDs.
    const std::size_t dataOffset(sizeof(uint64_t));

    BinaryPointTable table(m_celledSchema);
    pdal::PointRef pointRef(table, 0);

    Cell::PooledStack cellStack(m_pointPool.cellPool().acquire(numPoints));
    Data::PooledStack dataStack(m_pointPool.dataPool().acquire(numPoints));

    const std::size_t factor(m_metadata.structure().factor());

    const Climber startClimber(m_metadata.bbox(), m_metadata.structure());
    Climber climber(startClimber);

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

        climber = startClimber;
        climber.magnifyTo(cell->point(), curDepth);

        if (tube != normalize(climber.index()))
        {
            throw std::runtime_error("Bad serialized base tube");
        }

        insert(climber, cell);

        pos += celledPointSize;
    }
}

BaseChunk::~BaseChunk()
{
    Data::PooledStack dataStack(m_pointPool.dataPool());
    Cell::PooledStack cellStack(m_pointPool.cellPool());

    const std::size_t pointSize(m_metadata.schema().pointSize());
    Compressor compressor(m_celledSchema);

    std::vector<char> point(m_celledSchema.pointSize());

    const std::size_t tubeIdSize(sizeof(uint64_t));

    uint64_t i(0);
    const char* iPos(reinterpret_cast<char*>(&i));
    const char* iEnd(iPos + tubeIdSize);

    for ( ; i < m_tubes.size(); ++i)
    {
        Tube& tube(m_tubes[i]);

        for (auto& inner : tube)
        {
            Cell::PooledNode& cell(inner.second);

            for (const char* d : *cell)
            {
                std::copy(iPos, iEnd, point.data());
                std::copy(d, d + pointSize, point.data() + tubeIdSize);

                compressor.push(point.data(), point.size());
            }

            dataStack.push(cell->acquire());
            cellStack.push(std::move(cell));
        }
    }

    m_data = compressor.data();
    pushTail(*m_data, Tail(dataStack.size(), Type::Contiguous));
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
            throw std::runtime_error("Tube mismatch");
        }

        if (!theirs.empty())
        {
            ours.swap(std::move(theirs));
        }
    }
}

} // namespace entwine

