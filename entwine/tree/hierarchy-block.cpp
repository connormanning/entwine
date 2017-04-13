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

#include <entwine/types/metadata.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/io.hpp>
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
        const Id& maxPoints,
        const std::size_t size)
    : m_pool(pool)
    , m_metadata(metadata)
    , m_id(id)
    , m_ep(ep)
    , m_maxPoints(maxPoints)
    , m_size(size)
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
        const std::vector<char>& data,
        const bool readOnly)
{
    std::unique_ptr<std::vector<char>> decompressed;

    const auto compress(metadata.storage().hierarchyCompression());

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
    else if (!readOnly)
    {
        return makeUnique<SparseBlock>(
                pool,
                metadata,
                id,
                outEndpoint,
                maxPoints,
                decompressed ? *decompressed : data);
    }
    else
    {
        return makeUnique<ReadOnlySparseBlock>(
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
    auto data(combine());

    const auto type(m_metadata.storage().hierarchyCompression());
    if (type == HierarchyCompression::Lzma)
    {
        data = *Compression::compressLzma(data);
    }

    io::ensurePut(ep, m_id.str() + pf, data);
}

ContiguousBlock::ContiguousBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const std::size_t maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints, data.size())
    , m_tubes(maxPoints)
    , m_spinners(maxPoints)
{
    const char* pos(data.data());
    const char* end(data.data() + data.size());

    uint64_t tube, tick, cell;

    while (pos < end)
    {
        tube = extract(pos, end);
        tick = extract(pos, end);
        cell = extract(pos, end);

        m_tubes.at(tube).insert(std::make_pair(tick, m_pool.acquireOne(cell)));
    }
}

std::vector<char> ContiguousBlock::combine()
{
    std::vector<char> data;

    for (uint64_t tube(0); tube < m_tubes.size(); ++tube)
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
    : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints, data.size())
    , m_spinner()
    , m_tubes()
{
    parse(data.data(), data.data() + data.size());
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
            metadata.hierarchyStructure().baseIndexSpan(),
            0)
    , m_blocks()
{
    Structure s(m_metadata.hierarchyStructure());
    s.clearStart(); // Necessary for calcSpans.

    if (m_metadata.subset())
    {
        const std::vector<Subset::Span> spans(
                m_metadata.subset()->calcSpans(
                    s,
                    m_metadata.boundsNativeCubic()));

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

    const std::size_t factor(m_metadata.hierarchyStructure().factor());

    while (pos < end)
    {
        tube = extract(pos, end);
        tick = extract(pos, end);
        cell = extract(pos, end);

        const std::size_t depth(ChunkInfo::calcDepth(factor, m_id + tube));

        m_blocks.at(depth).count(m_id + tube, tick, cell);
    }
}

std::vector<char> BaseBlock::combine()
{
    // Pretty much the same as ContiguousBlock::combine, but normalized
    // relative to our own ID.
    std::vector<char> data;

    for (const auto& block : m_blocks)
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

    return data;
}

// TODO This is pretty much identical to BaseChunk::merge and should
// probably be moved templatized up into Splitter.
std::set<Id> BaseBlock::merge(BaseBlock& other)
{
    std::set<Id> ids;

    const auto& s(m_metadata.hierarchyStructure());
    const auto ppc(s.basePointsPerChunk());
    const std::size_t sharedDepth(
            m_metadata.subset() ?
                m_metadata.subset()->minimumNullDepth() : 0);

    for (std::size_t d(s.baseDepthBegin()); d < m_blocks.size(); ++d)
    {
        auto& block(m_blocks[d]);
        auto& adding(other.m_blocks[d]);

        if (d < sharedDepth) block.merge(adding);
        else block.append(adding);

        if (s.bumpDepth() && d >= s.bumpDepth())
        {
            if (block.maxPoints() == ppc)
            {
                const Id id(block.id());
                SparseBlock write(m_pool, m_metadata, id, m_ep, ppc);

                for (std::size_t i(0); i < block.tubes().size(); ++i)
                {
                    for (const auto& c : block.tubes().at(i))
                    {
                        write.count(id + i, c.first, c.second->val());
                    }
                }

                if (!write.tubes().empty())
                {
                    if (!m_ep)
                    {
                        throw std::runtime_error("Missing hierarchy endpoint");
                    }

                    write.save(*m_ep);
                    ids.insert(id);
                }

                block.clear();
            }
        }
    }

    return ids;
}

ReadOnlySparseBlock::ReadOnlySparseBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const Id& maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints, data.size())
{
    // Assuming that all the Id values are within a 64-bit range, then we have
    // four uint64 values per cell.
    m_data.reserve(data.size() / 32);
    parse(data.data(), data.data() + data.size());

    if (!std::is_sorted(m_data.begin(), m_data.end()))
    {
        std::cout << "Unsorted hierarchy block found" << std::endl;
        std::sort(m_data.begin(), m_data.end());
    }
}

} // namespace entwine

