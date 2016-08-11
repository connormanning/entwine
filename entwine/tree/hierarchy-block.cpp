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
    --chunkCount;
}

std::unique_ptr<HierarchyBlock> HierarchyBlock::create(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const Id& maxPoints)
{
    if (id < metadata.hierarchyStructure().sparseIndexBegin())
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
        return makeUnique<SparseBlock>(pool, metadata, id, outEndpoint);
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

    if (id < metadata.hierarchyStructure().sparseIndexBegin())
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

std::vector<char> ContiguousBlock::combine() const
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

void ContiguousBlock::merge(const HierarchyBlock& base)
{
    auto& other(dynamic_cast<const ContiguousBlock&>(base));

    for (std::size_t tube(0); tube < other.m_tubes.size(); ++tube)
    {
        for (const auto& cell : other.m_tubes[tube])
        {
            count(tube, cell.first, cell.second->val());
        }
    }
}

SparseBlock::SparseBlock(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const Id& id,
        const arbiter::Endpoint* outEndpoint,
        const std::vector<char>& data)
    : HierarchyBlock(pool, metadata, id, outEndpoint, 0)
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

std::vector<char> SparseBlock::combine() const
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

void SparseBlock::merge(const HierarchyBlock& base)
{
    auto& other(dynamic_cast<const SparseBlock&>(base));

    for (const auto& tPair : other.m_tubes)
    {
        const Id id(m_id + tPair.first);    // Count accepts non-normalized IDs.
        const HierarchyTube& tube(tPair.second);

        for (const auto& cell : tube)
        {
            count(id, cell.first, cell.second->val());
        }
    }
}

} // namespace entwine

