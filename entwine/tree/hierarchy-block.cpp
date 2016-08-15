/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/hierarchy-block.hpp>

#include <atomic>

#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
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

std::size_t HierarchyBlock::count() { return chunkCount; }

HierarchyBlock::HierarchyBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* ep,
        const Id& maxPoints)
    : m_pool(pool)
    , m_metadata(metadata)
    , m_id(id)
    , m_ep(ep)
    , m_maxPoints(maxPoints)
{
    ++chunkCount;
}

HierarchyBlock::~HierarchyBlock()
{
    if (chunkCount) --chunkCount;
}

std::unique_ptr<HierarchyBlock> HierarchyBlock::create(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const Id& maxPoints)
{
    if (!id)
    {
        return makeUnique<BaseBlock>(pool, metadata, outEndpoint);
    }
    if (id < metadata.hierarchyStructure().mappedIndexBegin())
    {
        return makeUnique<ContiguousBlock>(
                pool,
                metadata,
                id,
                outEndpoint,
                maxPoints.getSimple());
    }
    else
    {
        return makeUnique<SparseBlock>(
                pool,
                metadata,
                id,
                outEndpoint,
                maxPoints);
    }
}

std::unique_ptr<HierarchyBlock> HierarchyBlock::create(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const Id& maxPoints,
        const std::vector<char>& data)
{
    std::unique_ptr<std::vector<char>> decompressed;

    const auto compress(metadata.format().hierarchyCompression());

    if (compress == HierarchyCompression::Lzma)
    {
        decompressed = Compression::decompressLzma(data);
    }

    if (!id)
    {
        return makeUnique<BaseBlock>(
                pool,
                metadata,
                outEndpoint,
                decompressed ? *decompressed : data);
    }
    else if (id < metadata.hierarchyStructure().mappedIndexBegin())
    {
        return makeUnique<ContiguousBlock>(
                pool,
                metadata,
                id,
                outEndpoint,
                maxPoints.getSimple(),
                decompressed ? *decompressed : data);
    }
    else
    {
        return makeUnique<SparseBlock>(
                pool,
                metadata,
                id,
                outEndpoint,
                maxPoints,
                decompressed ? *decompressed : data);
    }
}

void HierarchyBlock::save(const arbiter::Endpoint& ep, const std::string pf)
{
    const auto data(combine());

    if (m_metadata.format().hierarchyCompression() ==
            HierarchyCompression::Lzma)
    {
        Storage::ensurePut(
                ep,
                m_id.str() + pf,
                *Compression::compressLzma(data));
    }
    else
    {
        Storage::ensurePut(ep, m_id.str() + pf, data);
    }
}

ContiguousBlock::ContiguousBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const std::size_t maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints)
    , m_tubes(maxPoints)
    , m_spinners(maxPoints)
{
    const char* pos(data.data());
    const char* end(data.data() + data.size());

    uint64_t tube, tick, cell;

    auto extract([&pos]()
    {
        const uint64_t v(*reinterpret_cast<const uint64_t*>(pos));
        pos += sizeof(uint64_t);
        return v;
    });

    while (pos < end)
    {
        tube = extract();
        tick = extract();
        cell = extract();

        m_tubes.at(tube).insert(std::make_pair(tick, m_pool.acquireOne(cell)));
    }
}

std::vector<char> ContiguousBlock::combine()
{
    std::vector<char> data;

    for (std::size_t tube(0); tube < m_tubes.size(); ++tube)
    {
        for (const auto& cell : m_tubes[tube])
        {
            push(data, tube);
            push(data, cell.first);
            push(data, cell.second->val());
        }
    }

    return data;
}

bool ContiguousBlock::empty() const
{
    for (const auto& tube : m_tubes)
    {
        if (!tube.empty()) return false;
    }

    return true;
}

SparseBlock::SparseBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const Id& maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints)
    , m_spinner()
    , m_tubes()
{
    const char* pos(data.data());
    const char* end(data.data() + data.size());

    const Id::Block* tubePos(nullptr);
    uint64_t tubeBlocks, tubeBytes, tick, cell;

    auto extract([&pos]()
    {
        const uint64_t v(*reinterpret_cast<const uint64_t*>(pos));
        pos += sizeof(uint64_t);
        return v;
    });

    while (pos < end)
    {
        tubeBlocks = extract();
        tubeBytes = tubeBlocks * sizeof(Id::Block);
        tubePos = reinterpret_cast<const Id::Block*>(pos);
        pos += tubeBytes;

        tick = extract();
        cell = extract();

        m_tubes[Id(tubePos, tubePos + tubeBlocks)].insert(
                std::make_pair(tick, m_pool.acquireOne(cell)));
    }
}

std::vector<char> SparseBlock::combine()
{
    std::vector<char> data;

    for (const auto& pair : m_tubes)
    {
        const Id& id(pair.first);
        const auto& tube(pair.second);

        for (const auto& cell : tube)
        {
            push(data, id.data().size());
            for (const Id::Block block : id.data()) push(data, block);
            push(data, cell.first);
            push(data, cell.second->val());
        }
    }

    return data;
}

BaseBlock::BaseBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const arbiter::Endpoint* outEndpoint)
    : HierarchyBlock(
            pool,
            metadata,
            0,
            outEndpoint,
            metadata.hierarchyStructure().baseIndexSpan())
    , m_blocks()
{
    Structure s(m_metadata.hierarchyStructure());
    s.clearStart(); // Necessary for calcSpans.

    if (m_metadata.subset())
    {
        const std::vector<Subset::Span> spans(
                m_metadata.subset()->calcSpans(s, m_metadata.bounds()));

        const std::size_t sharedDepth(m_metadata.subset()->minimumNullDepth());

        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
        {
            const std::size_t index(
                    d < sharedDepth ?
                        ChunkInfo::calcLevelIndex(2, d).getSimple() :
                        spans[d].begin());

            const std::size_t maxPoints(
                    d < sharedDepth ?
                        ChunkInfo::pointsAtDepth(2, d).getSimple() :
                        spans[d].end() - spans[d].begin());

            m_blocks.emplace_back(
                    pool,
                    metadata,
                    index,
                    outEndpoint,
                    maxPoints);
        }
    }
    else
    {
        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
        {
            m_blocks.emplace_back(
                    pool,
                    metadata,
                    ChunkInfo::calcLevelIndex(2, d),
                    outEndpoint,
                    ChunkInfo::pointsAtDepth(2, d).getSimple());
        }
    }
}

BaseBlock::BaseBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const arbiter::Endpoint* outEndpoint,
        const std::vector<char>& data)
    : BaseBlock(pool, metadata, outEndpoint)
{
    const char* pos(data.data());
    const char* end(data.data() + data.size());

    uint64_t tube, tick, cell;

    auto extract([&pos]()
    {
        const uint64_t v(*reinterpret_cast<const uint64_t*>(pos));
        pos += sizeof(uint64_t);
        return v;
    });

    const std::size_t factor(m_metadata.hierarchyStructure().factor());

    while (pos < end)
    {
        tube = extract();
        tick = extract();
        cell = extract();

        const std::size_t depth(ChunkInfo::calcDepth(factor, m_id + tube));

        m_blocks.at(depth).count(m_id + tube, tick, cell);
    }
}

std::vector<char> BaseBlock::combine()
{
    // Pretty much the same as ContiguousBlock::combine, but normalized
    // relative to our own ID.
    std::vector<char> data;

    makeWritable();

    for (const auto& write : m_writes)
    {
        for (const auto& block : write)
        {
            const auto& tubes(block.tubes());

            for (std::size_t tube(0); tube < tubes.size(); ++tube)
            {
                for (const auto& cell : tubes[tube])
                {
                    push(data, (block.id() + tube).getSimple());
                    push(data, cell.first);
                    push(data, cell.second->val());
                }
            }
        }
    }

    return data;
}

// TODO This is pretty much identical to BaseChunk::merge and should
// probably be moved templatized up into Splitter.
std::set<Id> BaseBlock::merge(BaseBlock& other)
{
    std::set<Id> ids;

    makeWritable();

    const auto& s(m_metadata.hierarchyStructure());

    const std::size_t sharedDepth(
            m_metadata.subset() ?
                m_metadata.subset()->minimumNullDepth() : 0);

    for (std::size_t d(s.baseDepthBegin()); d < m_writes.size(); ++d)
    {
        std::vector<ContiguousBlock>& write(m_writes[d]);
        ContiguousBlock& adding(other.m_blocks[d]);

        if (!write.empty())
        {
            if (d < sharedDepth)
            {
                if (write.size() != 1)
                {
                    throw std::runtime_error("Invalid shared depth size");
                }
                else if (write.front().id() != adding.id())
                {
                    throw std::runtime_error("Invalid shared depth id");
                }
                else if (write.front().endId() != adding.endId())
                {
                    throw std::runtime_error("Invalid shared depth span");
                }
            }
            else if (write.back().endId() != adding.id())
            {
                throw std::runtime_error(
                        "Hierarchy merges must be performed consecutively");
            }
        }

        if (d < sharedDepth) write.front().merge(adding);
        else write.emplace_back(std::move(adding));

        if (s.bumpDepth() && d >= s.bumpDepth())
        {
            const auto span(write.back().endId() - write.front().id());

            if (span == s.basePointsPerChunk())
            {
                const Id id(write.front().id());

                SparseBlock block(m_pool, m_metadata, id, m_ep, span);

                for (ContiguousBlock& piece : write)
                {
                    for (std::size_t t(0); t < piece.tubes().size(); ++t)
                    {
                        for (const auto& c : piece.tubes().at(t))
                        {
                            block.count(
                                    piece.id() + t,
                                    c.first,
                                    c.second->val());
                        }
                    }
                }

                if (!block.tubes().empty())
                {
                    if (!m_ep)
                    {
                        throw std::runtime_error("Missing hierarchy endpoint");
                    }

                    block.save(*m_ep);
                    ids.insert(id);
                }

                write.clear();
            }
        }
    }

    return ids;
}

void BaseBlock::makeWritable()
{
    if (m_writes.empty())
    {
        const auto& s(m_metadata.hierarchyStructure());
        m_writes.resize(s.baseDepthEnd());

        for (std::size_t i(s.baseDepthBegin()); i < m_writes.size(); ++i)
        {
            m_writes[i].emplace_back(std::move(m_blocks.at(i)));
        }
    }
}

} // namespace entwine

