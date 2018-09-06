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
/*

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <entwine/types/key.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>

namespace entwine
{

class NewClimber;

class Tube
{
public:
    // If result == true, then this cell has been consumed and may no longer be
    // accessed.
    //
    // Otherwise, the cell should be reinserted.  In this case, it's possible
    // that the cell was swapped with another - so cell values should not be
    // cached through calls to insert.
    bool insert(const Key& pk, Cell::PooledNode& cell)
    {
        const auto z(pk.position().z);

        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it(m_cells.find(z));

        if (it != m_cells.end())
        {
            Cell::PooledNode& curr(it->second);

            if (cell->point() != curr->point())
            {
                const Point& center(pk.bounds().mid());

                const auto a(cell->point().sqDist3d(center));
                const auto b(curr->point().sqDist3d(center));

                if (a < b || (a == b && ltChained(cell->point(), curr->point())))
                {
                    std::swap(cell, curr);
                }
            }
            else
            {
                it->second->push(
                        std::move(cell),
                        pk.metadata().schema().pointSize());
                return true;
            }
        }
        else
        {
            m_cells.emplace(std::make_pair(z, std::move(cell)));
            return true;
        }

        return false;
    }

    bool empty() const { return m_cells.empty(); }

    using CellMap = std::unordered_map<uint64_t, Cell::PooledNode>;

    CellMap::iterator begin() { return m_cells.begin(); }
    CellMap::iterator end() { return m_cells.end(); }
    CellMap::const_iterator begin() const { return m_cells.begin(); }
    CellMap::const_iterator end() const { return m_cells.end(); }

    Tube() = default;

    Tube(Tube&& other) noexcept
    {
        m_cells = std::move(other.m_cells);
    }

    Tube& operator=(Tube&& other) noexcept
    {
        m_cells = std::move(other.m_cells);
        return *this;
    }

private:
    CellMap m_cells;
    std::mutex m_mutex;
};

} // namespace entwine
*/

