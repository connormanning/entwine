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

#include <entwine/types/bbox.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Climber;

class Tube
{
public:
    void swap(Tube&& other) { std::swap(m_cells, other.m_cells); }

    // Insert with prior knowledge that it will succeed.  If there is already
    // a cell at the requested tick, it must be mergeable with the requested
    // cell or this will throw.
    void insert(std::size_t tick, Cell::PooledNode& cell);

    // Returns true if this insertion was successful and the cell has been
    // consumed.
    //
    // If false, the cell should be reinserted.  In this case, it's possible
    // that the cell was swapped with another - so cell values should not be
    // cached through calls to insert.
    bool insert(const Climber& climber, Cell::PooledNode& cell);

    using Cells = std::unordered_map<uint64_t, Cell::PooledNode>;

    bool empty() const { return m_cells.empty(); }
    static constexpr std::size_t maxTickDepth() { return 64; }

    static std::size_t calcTick(
            const Point& point,
            const BBox& bbox,
            const std::size_t depth)
    {
        return
            std::floor(
                    (point.z - bbox.min().z) * (1ULL << depth) /
                    (bbox.max().z - bbox.min().z));
    }

    Cells::iterator begin() { return m_cells.begin(); }
    Cells::iterator end() { return m_cells.end(); }
    Cells::const_iterator begin() const { return m_cells.begin(); }
    Cells::const_iterator end() const { return m_cells.end(); }

private:
    Cells m_cells;
    SpinLock m_spinner;
};

} // namespace entwine

