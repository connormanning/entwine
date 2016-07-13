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

#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<HierarchyBlock> HierarchyBlock::create(
        const Metadata& metadata,
        const Id& id,
        const Id& maxPoints)
{
    const auto compress(metadata.format().hierarchyCompression());
    if (id < metadata.structure().sparseIndexBegin())
    {
        return makeUnique<ContiguousBlock>(id, compress, maxPoints.getSimple());
    }
    else
    {
        return makeUnique<SparseBlock>(id, compress);
    }
}

std::unique_ptr<HierarchyBlock> HierarchyBlock::create(
        const Metadata& metadata,
        const Id& id,
        const Id& maxPoints,
        const std::vector<char>& data)
{
    std::unique_ptr<std::vector<char>> decompressed;

    const auto compress(metadata.format().hierarchyCompression());

    if (compress == HierarchyCompression::Lzma)
    {
        decompressed = Compression::decompressLzma(data);
    }

    if (id < metadata.structure().sparseIndexBegin())
    {
        return makeUnique<ContiguousBlock>(
                id,
                compress,
                maxPoints.getSimple(),
                decompressed ? *decompressed : data);
    }
    else
    {
        return makeUnique<SparseBlock>(
                id,
                compress,
                decompressed ? *decompressed : data);
    }
}

void HierarchyBlock::save(const arbiter::Endpoint& ep, const std::string pf)
{
    const auto data(combine());

    if (m_compress == HierarchyCompression::Lzma)
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
        const Id& id,
        const HierarchyCompression c,
        const std::size_t maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(id, c)
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

        m_tubes.at(tube)[tick] = cell;
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
            push(data, cell.second.val());
        }
    }

    return data;
}

void ContiguousBlock::merge(const ContiguousBlock& other)
{
    for (std::size_t tube(0); tube < m_tubes.size(); ++tube)
    {
        for (const auto& cell : other.m_tubes[tube])
        {
            count(tube, cell.first, cell.second.val());
        }
    }
}

SparseBlock::SparseBlock(
        const Id& id,
        const HierarchyCompression c,
        const std::vector<char>& data)
    : HierarchyBlock(id, c)
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

        m_tubes[Id(tubePos, tubePos + tubeBlocks)][tick] = cell;
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
            push(data, cell.second.val());
        }
    }

    return data;
}

} // namespace entwine

