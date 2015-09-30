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
#include <entwine/types/schema.hpp>

namespace entwine
{

typedef std::atomic<PointInfo*> PointInfoAtom;

class Cell
{
public:
    Cell() : m_atom(0) { }
    Cell(std::unique_ptr<PointInfo> pointInfo)
        : m_atom(pointInfo.release())
    { }

    ~Cell()
    {
        if (m_atom.load()) delete m_atom.load();
    }

    const PointInfoAtom& atom() const
    {
        return m_atom;
    }

    bool swap(std::unique_ptr<PointInfo> newVal, PointInfo* oldVal = 0)
    {
        return m_atom.compare_exchange_weak(oldVal, newVal.release());
    }

    void store(std::unique_ptr<PointInfo> newVal)
    {
        m_atom.store(newVal.release());
    }

private:
    PointInfoAtom m_atom;
};

class Tube
{
public:
    Tube();

    std::pair<bool, Cell&> getCell(std::size_t tick);
    void addCell(std::size_t tick, std::unique_ptr<PointInfo> info);

    void save(
            const Schema& celledSchema,
            uint64_t tubeId,
            std::vector<char>& data) const;

    typedef std::unordered_map<uint64_t, Cell> MapType;

    bool empty() const;
    const Cell& primaryCell() const { return m_primaryCell; }
    const MapType& secondaryCells() const { return m_cells; }

private:
    std::pair<bool, Cell&> getMappedCell(std::size_t tick);

    std::atomic_size_t m_primaryTick;
    Cell m_primaryCell;

    MapType m_cells;
    std::mutex m_mutex;
};

} // namespace entwine

