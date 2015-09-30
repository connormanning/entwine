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

void Tube::addCell(const std::size_t tick, std::unique_ptr<PointInfo> info)
{
    if (m_primaryTick == unassigned)
    {
        m_primaryTick = tick;
        m_primaryCell.store(std::move(info));
    }
    else
    {
        m_cells.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(tick),
                std::forward_as_tuple(std::move(info)));
    }
}

std::pair<bool, Cell&> Tube::getCell(const std::size_t tick)
{
    if (tick == m_primaryTick)
    {
        return std::pair<bool, Cell&>(false, m_primaryCell);
    }

    std::size_t unassignedMut(unassigned);

    if (
            m_primaryTick == unassigned &&
            m_primaryTick.compare_exchange_strong(unassignedMut, tick))
    {
        return std::pair<bool, Cell&>(true, m_primaryCell);
    }
    else
    {
        return getMappedCell(tick);
    }
}

std::pair<bool, Cell&> Tube::getMappedCell(const std::size_t tick)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it(m_cells.find(tick));

    if (it == m_cells.end())
    {
        return std::pair<bool, Cell&>(true, m_cells[tick]);
    }
    else
    {
        return std::pair<bool, Cell&>(false, it->second);
    }
}

bool Tube::empty() const
{
    return m_primaryTick.load() == unassigned;
}

void Tube::save(
        const Schema& celledSchema,
        const uint64_t tubeId,
        std::vector<char>& data) const
{
    if (m_primaryTick.load() != unassigned)
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

            const std::vector<char>& rawVec(cell.atom().load()->data());
            const char* rawData(rawVec.data());

            std::copy(tubePos, tubePos + idSize, pos);
            std::copy(rawData, rawData + nativeSize, pos + idSize);

            pos += celledSize;
        }

        // Save mapped cells.
        for (const auto& c : m_cells)
        {
            const Cell& cell(c.second);

            const std::vector<char>& rawVec(cell.atom().load()->data());
            const char* rawData(rawVec.data());

            std::copy(tubePos, tubePos + idSize, pos);
            std::copy(rawData, rawData + nativeSize, pos + idSize);

            pos += celledSize;
        }
    }
}

} // namespace entwine

