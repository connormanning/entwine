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
#include <cstdlib>
#include <ctime>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>
#include <pdal/io/LasWriter.hpp>

#include <entwine/formats/cesium/tileset.hpp>
#include <entwine/formats/cesium/tile-builder.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    std::atomic_size_t chunkCount(0);
}

std::size_t Chunk::count() { return chunkCount; }

Chunk::Chunk(
        const Builder& builder,
        const Bounds& bounds,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints)
    : m_builder(builder)
    , m_metadata(m_builder.metadata())
    , m_bounds(bounds)
    , m_pointPool(m_builder.pointPool())
    , m_depth(depth)
    , m_zDepth(std::min(Tube::maxTickDepth(), depth))
    , m_id(id)
    , m_maxPoints(maxPoints)
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

void Chunk::save()
{
    m_metadata.format().serialize(*this);
}

Chunk::~Chunk()
{
    if (chunkCount) --chunkCount;
}

std::unique_ptr<Chunk> Chunk::create(
        const Builder& builder,
        const Bounds& bounds,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        const bool exists)
{
    if (depth && id < builder.metadata().structure().mappedIndexBegin())
    {
        return makeUnique<ContiguousChunk>(
                builder,
                bounds,
                depth,
                id,
                maxPoints,
                exists);
    }
    else if (depth)
    {
        return makeUnique<SparseChunk>(
                builder,
                bounds,
                depth,
                id,
                maxPoints,
                exists);
    }
    else
    {
        return makeUnique<BaseChunk>(builder, exists);
    }
}

///////////////////////////////////////////////////////////////////////////////

SparseChunk::SparseChunk(
        const Builder& builder,
        const Bounds& bounds,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        const bool exists)
    : Chunk(builder, bounds, depth, id, maxPoints)
{
    if (exists)
    {
        populate(format().deserialize(builder.outEndpoint(), m_pointPool, id));
    }
}

SparseChunk::~SparseChunk() { }

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

cesium::TileInfo SparseChunk::info() const
{
    std::map<std::size_t, std::size_t> ticks;
    std::size_t cur(0);
    const std::size_t div(divisor());

    for (const auto& tubePair : m_tubes)
    {
        for (const auto& cellPair : tubePair.second)
        {
            cur = cellPair.first / div;
            if (ticks.count(cur)) ticks[cur] += cellPair.second->size();
            else ticks[cur] = cellPair.second->size();
        }
    }

    return cesium::TileInfo(m_id, ticks, m_depth, m_bounds);
}

void SparseChunk::tile() const
{
    const cesium::TileInfo tileInfo(info());
    const auto endpoint(m_builder.outEndpoint().getSubEndpoint("cesium"));
    cesium::TileBuilder tileBuilder(m_metadata, tileInfo);

    for (const auto& tubePair : m_tubes)
    {
        for (const auto& cellPair : tubePair.second)
        {
            tileBuilder.push(cellPair.first, *cellPair.second);
        }
    }

    for (const auto& tilePair : tileBuilder.data())
    {
        const std::size_t tick(tilePair.first);
        const auto& tileData(tilePair.second);

        cesium::Tile tile(tileData.points, tileData.colors);

        Storage::ensurePut(
                endpoint,
                m_id.str() + "-" + std::to_string(tick) + ".pnts",
                tile.asBinary());
    }
}

///////////////////////////////////////////////////////////////////////////////

ContiguousChunk::ContiguousChunk(
        const Builder& builder,
        const Bounds& bounds,
        const std::size_t depth,
        const Id& id,
        const Id& maxPoints,
        const bool exists)
    : Chunk(builder, bounds, depth, id, maxPoints)
    , m_tubes(maxPoints.getSimple())
{
    if (exists)
    {
        populate(format().deserialize(builder.outEndpoint(), m_pointPool, id));
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

cesium::TileInfo ContiguousChunk::info() const
{
    std::map<std::size_t, std::size_t> ticks;
    std::size_t cur(0);
    const std::size_t div(divisor());
    const bool inBase(m_depth < m_metadata.structure().coldDepthBegin());

    for (const auto& tube : m_tubes)
    {
        for (const auto& cellPair : tube)
        {
            if (!inBase) cur = cellPair.first / div;
            if (ticks.count(cur)) ticks[cur] += cellPair.second->size();
            else ticks[cur] = cellPair.second->size();
        }
    }

    return cesium::TileInfo(m_id, ticks, m_depth, m_bounds);
}

void ContiguousChunk::tile() const
{
    const cesium::TileInfo tileInfo(info());
    const auto endpoint(m_builder.outEndpoint().getSubEndpoint("cesium"));
    const bool inBase(m_depth < m_metadata.structure().coldDepthBegin());
    cesium::TileBuilder tileBuilder(m_metadata, tileInfo);

    for (const auto& tube : m_tubes)
    {
        for (const auto& cellPair : tube)
        {
            tileBuilder.push(inBase ? 0 : cellPair.first, *cellPair.second);
        }
    }

    for (const auto& tilePair : tileBuilder.data())
    {
        const std::size_t tick(tilePair.first);
        const auto& tileData(tilePair.second);

        cesium::Tile tile(tileData.points, tileData.colors);

        Storage::ensurePut(
                endpoint,
                m_id.str() + "-" + std::to_string(tick) + ".pnts",
                tile.asBinary());
    }
}

///////////////////////////////////////////////////////////////////////////////

cesium::TileInfo BaseChunk::info() const
{
    throw std::runtime_error("Cannot call info on base");
}

std::vector<cesium::TileInfo> BaseChunk::baseInfo() const
{
    std::vector<cesium::TileInfo> result;

    const auto& s(m_metadata.structure());
    std::map<std::size_t, std::size_t> ticks;
    ticks[0] = 1;

    for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
    {
        if (d > s.nominalChunkDepth())
        {
            const std::size_t tickMax(1 << (d - s.nominalChunkDepth()));
            for (std::size_t t(0); t < tickMax; ++t) ticks[t] = 1;
        }

        result.emplace_back(m_chunks.at(d).id(), ticks, d, m_bounds);
    }

    return result;
}

void BaseChunk::tile() const
{
    for (
            std::size_t i(m_metadata.structure().baseDepthBegin());
            i < m_metadata.structure().baseDepthEnd();
            ++i)
    {
        m_chunks.at(i).tile();
    }
}

BaseChunk::BaseChunk(const Builder& builder, const bool exists)
    : Chunk(
            builder,
            builder.metadata().boundsScaledCubic(),
            builder.metadata().structure().baseDepthBegin(),
            builder.metadata().structure().baseIndexBegin(),
            builder.metadata().structure().baseIndexSpan())
    , m_celledSchema(Schema::makeCelled(m_metadata.schema()))
    , m_celledPool(m_celledSchema, m_metadata.delta())
{
    std::srand(std::time(0));
    const auto& s(m_metadata.structure());

    // These will go unused, but keep our API uniform, and let us avoid
    // subtracting offsets all the time.
    for (std::size_t d(0); d < s.baseDepthBegin(); ++d)
    {
        m_chunks.emplace_back(
                m_builder,
                m_metadata.boundsScaledCubic(),
                d,
                ChunkInfo::calcLevelIndex(2, d),
                0,
                false);
    }

    if (m_metadata.subset())
    {
        const std::vector<Subset::Span> spans(
                m_metadata.subset()->calcSpans(
                    m_metadata.structure(),
                    m_metadata.boundsNativeCubic()));

        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
        {
            m_chunks.emplace_back(
                    m_builder,
                    m_metadata.boundsScaledCubic(),
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
                    m_metadata.boundsScaledCubic(),
                    d,
                    ChunkInfo::calcLevelIndex(2, d),
                    ChunkInfo::pointsAtDepth(2, d),
                    false);
        }
    }

    chunkCount = 1;

    if (exists)
    {
        populate(format().deserialize(
                    builder.outEndpoint(),
                    m_celledPool,
                    m_id));
    }
}

Cell::PooledStack BaseChunk::acquire()
{
    Cell::PooledStack cellStack(m_celledPool.cellPool());
    BinaryPointTable table(m_celledSchema);
    pdal::PointRef pointRef(table, 0);

    const std::size_t tubeIdSize(sizeof(uint64_t));
    uint64_t tubeId(0);
    const char* tPos(reinterpret_cast<char*>(&tubeId));
    const char* tEnd(tPos + tubeIdSize);

    const std::size_t nativePointSize(m_metadata.schema().pointSize());

    for (auto& chunk : m_chunks)
    {
        for (std::size_t i(0); i < chunk.m_tubes.size(); ++i)
        {
            Tube& tube(chunk.m_tubes[i]);
            for (auto& inner : tube)
            {
                Cell::PooledNode& cell(inner.second);
                for (const char* d : *cell)
                {
                    auto cellNode(m_celledPool.cellPool().acquireOne());
                    auto dataNode(m_celledPool.dataPool().acquireOne());

                    tubeId = chunk.id().getSimple() + i - m_id.getSimple();
                    std::copy(d, d + nativePointSize, *dataNode);
                    std::copy(tPos, tEnd, *dataNode + nativePointSize);

                    table.setPoint(*dataNode);
                    cellNode->set(pointRef, std::move(dataNode));
                    cellStack.push(std::move(cellNode));
                }
            }
        }
    }

    return cellStack;
}

void BaseChunk::populate(Cell::PooledStack cells)
{
    const std::size_t numPoints(cells.size());
    Data::PooledStack dataStack(m_pointPool.dataPool().acquire(numPoints));
    Cell::PooledStack cellStack(m_pointPool.cellPool().acquire(numPoints));

    const std::size_t nativePointSize(m_metadata.schema().pointSize());
    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const auto factor(m_metadata.structure().factor());

    uint64_t tubeId(0);
    char* tPos(reinterpret_cast<char*>(&tubeId));

    BinaryPointTable table(m_metadata.schema());
    pdal::PointRef pointRef(table, 0);

    Climber climber(m_metadata);

    for (const auto& in : cells)
    {
        Cell::PooledNode cellNode(cellStack.popOne());
        Data::PooledNode dataNode(dataStack.popOne());

        const char* pos(in.uniqueData());
        std::copy(pos, pos + nativePointSize, *dataNode);
        std::copy(pos + nativePointSize, pos + celledPointSize, tPos);

        table.setPoint(*dataNode);
        cellNode->set(pointRef, std::move(dataNode));

        const std::size_t curDepth(ChunkInfo::calcDepth(factor, m_id + tubeId));
        climber.reset();
        climber.magnifyTo(cellNode->point(), curDepth);

        if (tubeId != (climber.index() - m_id).getSimple())
        {
            throw std::runtime_error("Bad serialized base tube");
        }

        insert(climber, cellNode);
    }
}

std::set<Id> BaseChunk::merge(BaseChunk& other)
{
    std::set<Id> ids;

    const auto& s(m_metadata.structure());

    for (std::size_t d(s.baseDepthBegin()); d < m_chunks.size(); ++d)
    {
        ContiguousChunk& chunk(m_chunks[d]);
        ContiguousChunk& toAdd(other.m_chunks[d]);

        if (chunk.endId() != toAdd.id())
        {
            throw std::runtime_error("Merges must be performed consecutively");
        }

        chunk.append(toAdd);

        if (s.bumpDepth() && d >= s.bumpDepth())
        {
            if (chunk.maxPoints() == s.basePointsPerChunk())
            {
                if (!chunk.empty())
                {
                    m_metadata.format().serialize(chunk);
                    ids.insert(chunk.id());
                }

                chunk.clear();
            }
        }
    }

    return ids;
}

} // namespace entwine

