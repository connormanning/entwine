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
#include <entwine/types/subset.hpp>
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

    if (chunkCount) --chunkCount;
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
            return makeUnique<BaseChunk>(builder);
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
        return makeUnique<BaseChunk>(builder, std::move(unpacker));
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
        const Id& maxPoints,
        const bool autosave)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
    , m_autosave(autosave)
{ }

ContiguousChunk::ContiguousChunk(
        const Builder& builder,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        Cell::PooledStack cells)
    : Chunk(builder, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
    , m_autosave(true)
{
    populate(std::move(cells));
}

ContiguousChunk::~ContiguousChunk()
{
    if (m_autosave) collect(ChunkType::Contiguous);
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

BaseChunk::BaseChunk(const Builder& builder)
    : Chunk(
            builder,
            builder.metadata().structure().baseDepthBegin(),
            builder.metadata().structure().baseIndexBegin(),
            builder.metadata().structure().baseIndexSpan())
    , m_chunks()
    , m_celledSchema(makeCelled(m_metadata.schema()))
    , m_writes()
{
    const auto& s(m_metadata.structure());

    // These will go unused, but keep our API uniform, and let us avoid
    // subtracting offsets all the time.
    for (std::size_t d(0); d < s.baseDepthBegin(); ++d)
    {
        m_chunks.emplace_back(
                m_builder,
                d,
                ChunkInfo::calcLevelIndex(2, d),
                0,
                false);
    }

    if (m_metadata.subset())
    {
        const std::vector<Subset::Span> spans(
                m_metadata.subset()->calcSpans(m_metadata, s.baseDepthEnd()));

        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
        {
            m_chunks.emplace_back(
                    m_builder,
                    d,
                    spans[d].begin(),
                    spans[d].end() - spans[d].begin(),
                    false);
        }
    }
    else
    {
        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
        {
            m_chunks.emplace_back(
                    m_builder,
                    d,
                    ChunkInfo::calcLevelIndex(2, d),
                    ChunkInfo::pointsAtDepth(2, d),
                    false);
        }
    }

    chunkCount = 1;
}

BaseChunk::BaseChunk(const Builder& builder, Unpacker unpacker)
    : BaseChunk(builder)
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

        if (tube != (climber.index() - m_id).getSimple())
        {
            throw std::runtime_error("Bad serialized base tube");
        }

        insert(climber, cell);

        pos += celledPointSize;
    }
}

void BaseChunk::save(const arbiter::Endpoint& endpoint)
{
    makeWriteable();

    Data::PooledStack dataStack(m_pointPool.dataPool());
    Cell::PooledStack cellStack(m_pointPool.cellPool());

    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const std::size_t nativePointSize(m_metadata.schema().pointSize());

    std::vector<char> point(celledPointSize);

    const std::size_t tubeIdSize(sizeof(uint64_t));

    const bool compress(m_metadata.format().compress());
    std::unique_ptr<Compressor> compressor(
            compress ? makeUnique<Compressor>(m_celledSchema) : nullptr);

    std::unique_ptr<std::vector<char>> data(makeUnique<std::vector<char>>());

    uint64_t tubeId(0);
    const char* tPos(reinterpret_cast<char*>(&tubeId));
    const char* tEnd(tPos + tubeIdSize);

    for (auto& w : m_writes) for (auto& c : w)
    {
        auto& tubes(c.tubes());

        for (std::size_t i = 0; i < tubes.size(); ++i)
        {
            Tube& tube(tubes[i]);

            for (auto& inner : tube)
            {
                Cell::PooledNode& cell(inner.second);

                for (const char* d : *cell)
                {
                    tubeId = c.id().getSimple() + i - m_id.getSimple();
                    std::copy(tPos, tEnd, point.data());
                    std::copy(
                            d,
                            d + nativePointSize,
                            point.data() + tubeIdSize);

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

void BaseChunk::makeWriteable()
{
    if (m_writes.empty())
    {
        const auto& s(m_metadata.structure());
        m_writes.resize(s.baseDepthEnd());

        for (std::size_t i(s.baseDepthBegin()); i < m_writes.size(); ++i)
        {
            m_writes[i].emplace_back(std::move(m_chunks[i]));
        }
    }
}

std::set<Id> BaseChunk::merge(BaseChunk& other)
{
    std::set<Id> ids;

    makeWriteable();

    const auto& s(m_metadata.structure());

    for (std::size_t d(s.baseDepthBegin()); d < m_writes.size(); ++d)
    {
        std::vector<ContiguousChunk>& write(m_writes[d]);
        ContiguousChunk& a(write.back());
        ContiguousChunk& b(other.m_chunks[d]);

        if (a.endId() != b.id())
        {
            throw std::runtime_error("Merges must be performed consecutively");
        }

        write.emplace_back(std::move(other.m_chunks[d]));

        if (s.bumpDepth() && d >= s.bumpDepth())
        {
            const auto span(write.back().endId() - write.front().id());

            if (span == s.basePointsPerChunk())
            {
                const Id id(write.front().id());
                ContiguousChunk chunk(m_builder, d, id, s.basePointsPerChunk());

                chunk.tubes().clear();
                ids.insert(id);

                for (ContiguousChunk& piece : write)
                {
                    chunk.tubes().insert(
                            chunk.tubes().end(),
                            std::make_move_iterator(piece.tubes().begin()),
                            std::make_move_iterator(piece.tubes().end()));
                }

                write.clear();

                std::cout << "\tCombined at " << d << ": " << id << std::endl;
            }
        }
    }

    return ids;
}

Cell::PooledStack BaseChunk::acquire()
{
    return Cell::PooledStack(m_pointPool.cellPool());
}

} // namespace entwine

