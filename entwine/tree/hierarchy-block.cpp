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

namespace entwine
{

ContiguousBlock::ContiguousBlock(
        const Id& id,
        std::size_t maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(id)
    , m_tubes(maxPoints)
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

void ContiguousBlock::save(const arbiter::Endpoint& ep, std::string pf)
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

    if (data.size()) ep.put(m_id.str() + pf, data);
}

SparseBlock::SparseBlock(
        const Id& id,
        std::size_t maxPoints,
        const std::vector<char>& data)
    : HierarchyBlock(id)
    , m_tubes()
{
    const char* pos(data.data());
    const char* end(data.data() + data.size());

    const Id::Block* tubePos(nullptr);
    uint64_t tubeSize, tick, cell;

    auto extract([&pos]()
    {
        const uint64_t v(*reinterpret_cast<const uint64_t*>(pos));
        pos += sizeof(uint64_t);
        return v;
    });

    while (pos < end)
    {
        tubeSize = extract();
        tubePos = reinterpret_cast<const Id::Block*>(pos);
        pos += tubeSize;

        tick = extract();
        cell = extract();

        m_tubes[Id(tubePos, tubePos + tubeSize)][tick] = cell;
    }
}

void SparseBlock::save(const arbiter::Endpoint& ep, std::string pf)
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

    if (data.size()) ep.put(m_id.str() + pf, data);
}

} // namespace entwine

