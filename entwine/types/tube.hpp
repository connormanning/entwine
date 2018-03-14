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

#include <entwine/types/point-pool.hpp>

namespace entwine
{

class NewClimber;

class Tube
{
public:
    class Insertion
    {
    public:
        Insertion() { }
        Insertion(bool done, int delta) : m_done(done) , m_delta(delta) { }

        bool done() const { return m_done; }
        int delta() const { return m_delta; }

        void setDelta(int delta) { m_delta = delta; }
        void setDone(int delta) { m_done = true; m_delta = delta; }

    private:
        bool m_done = false;
        int m_delta = 0;
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
    Insertion insert(const NewClimber& climber, Cell::PooledNode& cell);

    bool empty() const { return m_cells.empty(); }
    static constexpr std::size_t maxTickDepth() { return 64; }

    using Cells = std::map<uint64_t, Cell::PooledNode>;

    Cells::iterator begin() { return m_cells.begin(); }
    Cells::iterator end() { return m_cells.end(); }
    Cells::const_iterator begin() const { return m_cells.begin(); }
    Cells::const_iterator end() const { return m_cells.end(); }

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
    Cells m_cells;
    std::mutex m_mutex;
};

} // namespace entwine

