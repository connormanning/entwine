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

typedef std::atomic<RawInfoNode*> PointInfoAtom;

class Cell
{
public:
    Cell() : m_atom(nullptr) { }
    Cell(PooledInfoNode& pointInfo) : m_atom() { store(pointInfo); }

    Cell(const Cell& other)
        : m_atom(other.atom().load())
    { }

    Cell& operator=(const Cell& other)
    {
        m_atom.store(other.atom().load());
        return *this;
    }

    const PointInfoAtom& atom() const
    {
        return m_atom;
    }

    bool swap(PooledInfoNode& newPooled, RawInfoNode* oldVal = 0)
    {
        if (m_atom.compare_exchange_weak(oldVal, newPooled.get()))
        {
            newPooled.release();
            return true;
        }
        else
        {
            return false;
        }
    }

    void store(PooledInfoNode& newPooled)
    {
        RawInfoNode* val(newPooled.release());
        m_atom.store(val);
    }

private:
    PointInfoAtom m_atom;
};

class Tube
{
public:
    Tube();
    Tube& operator=(const Tube& other)
    {
        m_primaryTick.store(other.primaryTick());
        m_primaryCell = other.primaryCell();
        m_cells = other.secondaryCells();

        return *this;
    }

    std::pair<bool, Cell&> getCell(std::size_t tick);
    void addCell(std::size_t tick, PooledInfoNode info);

    void save(
            const Schema& celledSchema,
            uint64_t tubeId,
            std::vector<char>& data,
            PooledDataStack& dataStack,
            PooledInfoStack& infoStack) const;

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
    std::atomic_size_t m_primaryTick;
    Cell m_primaryCell;

    MapType m_cells;
    std::mutex m_mutex;
};

} // namespace entwine

