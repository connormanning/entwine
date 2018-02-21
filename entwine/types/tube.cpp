/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/new-climber.hpp>
#include <entwine/types/tube.hpp>

namespace entwine
{

Tube::Insertion Tube::insert(const NewClimber& climber, Cell::PooledNode& cell)
{
    Insertion result;

    std::lock_guard<std::mutex> lock(m_mutex);

    const auto& pk(climber.pointKey());
    const auto z(pk.z);
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
                // We are inserting cell, and extracting curr.  Store our new
                // cell, and send the previous one further down the tree.
                result.setDelta(
                        static_cast<int>(cell->size()) -
                        static_cast<int>(curr->size()));
                std::swap(cell, curr);
            }
            // Else, the default-constructed result is correct.
        }
        else
        {
            result.setDone(cell->size());
            it->second->push(std::move(cell), climber.pointSize());
        }
    }
    else
    {
        result.setDone(cell->size());
        m_cells.emplace(std::make_pair(z, std::move(cell)));
    }

    return result;
}

} // namespace entwine

