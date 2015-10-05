/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <entwine/tree/point-info.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

typedef std::atomic<PooledInfoNode*> PointInfoAtom;

class Cell
{
public:
    Cell() : m_atom(0) { }
    Cell(PooledInfoNode* pointInfo)
        : m_atom(pointInfo)
    { }

    ~Cell()
    {
    }

    const PointInfoAtom& atom() const
    {
        return m_atom;
    }

    bool swap(PooledInfoNode* newVal, PooledInfoNode* oldVal = 0)
    {
        return m_atom.compare_exchange_weak(oldVal, newVal);
    }

    void store(PooledInfoNode* newVal)
    {
        m_atom.store(newVal);
    }

private:
    PointInfoAtom m_atom;
};

class Tube
{
public:
    Tube();

    std::pair<bool, Cell&> getCell(std::size_t tick);
    void addCell(std::size_t tick, PooledInfoNode* info);

    void save(
            const Schema& celledSchema,
            uint64_t tubeId,
            std::vector<char>& data,
            PooledStack& stack) const;

    typedef std::unordered_map<uint64_t, Cell> MapType;

    bool empty() const;
    std::size_t primaryTick() const { return m_primaryTick; }
    const Cell& primaryCell() const { return m_primaryCell; }
    const MapType& secondaryCells() const { return m_cells; }

    static std::size_t calcTick(
            const Point& point,
            const BBox& bbox,
            std::size_t depth)
    {
        return
            static_cast<std::size_t>(
                    std::floor((point.z - bbox.min().z) * (1ULL << depth)) /
                    (bbox.max().z - bbox.min().z));
    }


private:
    std::pair<bool, Cell&> getMappedCell(std::size_t tick);

    std::atomic_size_t m_primaryTick;
    Cell m_primaryCell;

    MapType m_cells;
    std::mutex m_mutex;
};

} // namespace entwine

