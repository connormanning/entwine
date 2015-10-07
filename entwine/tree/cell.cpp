/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/cell.hpp>

namespace entwine
{

namespace
{
    const std::size_t unassigned(std::numeric_limits<std::size_t>::max());
}

Tube::Tube()
    : m_primaryTick(unassigned)
    , m_primaryCell()
    , m_cells()
    , m_mutex()
{ }

void Tube::addCell(const std::size_t tick, PooledInfoNode* info)
{
    if (m_primaryTick == unassigned)
    {
        m_primaryTick = tick;
        m_primaryCell.store(info);
    }
    else
    {
        m_cells.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(tick),
                std::forward_as_tuple(info));
    }
}

std::pair<bool, Cell&> Tube::getCell(const std::size_t tick)
{
    if (tick == m_primaryTick.load())
    {
        return std::pair<bool, Cell&>(false, m_primaryCell);
    }

    std::size_t unassignedMut(unassigned);
    const bool assigned(
            m_primaryTick.load() == unassigned &&
            m_primaryTick.compare_exchange_strong(unassignedMut, tick));

    // It's possible that another thread beat us to the swap, and successfully
    // swapped with the same value as our incoming tick.  Reload after the swap
    // attempt to counter this, since if our _assigned_ statement failed to
    // swap, then our primary tick is now set to its final value.
    if (assigned || m_primaryTick.load() == tick)
    {
        return std::pair<bool, Cell&>(assigned, m_primaryCell);
    }
    else
    {
        // Primary tick already assigned, and it's not the incoming one.  Go to
        // secondary cells.
        std::lock_guard<std::mutex> lock(m_mutex);
        const bool added(!m_cells.count(tick));
        return std::pair<bool, Cell&>(added, m_cells[tick]);
    }
}

bool Tube::empty() const
{
    return m_primaryTick.load() == unassigned;
}

void Tube::save(
        const Schema& celledSchema,
        const uint64_t tubeId,
        std::vector<char>& data,
        PooledStack& stack) const
{
    if (!empty())
    {
        const std::size_t idSize(sizeof(uint64_t));

        const std::size_t celledSize(celledSchema.pointSize());
        const std::size_t nativeSize(celledSize - idSize);

        // Include space for primary cell.
        data.resize(celledSize + m_cells.size() * celledSize);
        char* pos(data.data());

        const char* tubePos(reinterpret_cast<const char*>(&tubeId));

        // TODO Deduplicate.

        // Save primary cell.
        {
            const Cell& cell(m_primaryCell);
            const char* rawData(cell.atom().load()->val().data());

            std::copy(tubePos, tubePos + idSize, pos);
            std::copy(rawData, rawData + nativeSize, pos + idSize);

            stack.push(cell.atom().load());

            pos += celledSize;
        }

        // Save mapped cells.
        for (const auto& c : m_cells)
        {
            const Cell& cell(c.second);
            const char* rawData(cell.atom().load()->val().data());

            std::copy(tubePos, tubePos + idSize, pos);
            std::copy(rawData, rawData + nativeSize, pos + idSize);

            stack.push(cell.atom().load());

            pos += celledSize;
        }
    }
}

} // namespace entwine

