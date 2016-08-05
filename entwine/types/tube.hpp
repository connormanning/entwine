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
#include <map>
#include <mutex>
#include <vector>

#include <entwine/types/bounds.hpp>
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

    class Insertion
    {
    public:
        Insertion() : m_done(false), m_delta(0) { }

        Insertion(bool done, int delta)
            : m_done(done)
            , m_delta(delta)
        { }

        bool done() const { return m_done; }
        int delta() const { return m_delta; }

        void setDelta(int delta) { m_delta = delta; }
        void setDone(int delta) { m_done = true; m_delta = delta; }

    private:
        bool m_done;
        int m_delta;
    };

    // If result.done() == true, then this cell has been consumed and may no
    // longer be accessed.
    //
    // The value of result.delta() is equal to (pointsInserted - pointsRemoved),
    // which may be any value if result.done() == false.
    //
    // If result.done() == false, the cell should be reinserted.  In this case,
    // it's possible that the cell was swapped with another - so cell values
    // should not be cached through calls to insert.
    Insertion insert(const Climber& climber, Cell::PooledNode& cell);

    using Cells = std::map<uint64_t, Cell::PooledNode>;

    bool empty() const { return m_cells.empty(); }
    static constexpr std::size_t maxTickDepth() { return 64; }

    static std::size_t calcTick(
            const Point& point,
            const Bounds& bounds,
            const std::size_t depth)
    {
        return
            std::floor(
                    (point.z - bounds.min().z) * (1ULL << depth) /
                    (bounds.max().z - bounds.min().z));
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

