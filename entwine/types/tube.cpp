/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/climber.hpp>
#include <entwine/types/tube.hpp>

namespace entwine
{

void Tube::insert(std::size_t tick, Cell::PooledNode& cell)
{
    SpinGuard lock(m_spinner);

    const auto it(m_cells.find(tick));

    if (it != m_cells.end())
    {
        if (cell->point() != it->second->point())
        {
            throw std::runtime_error("Invalid serialized chunk point");
        }

        it->second->push(std::move(cell));
    }
    else
    {
        m_cells.emplace(std::make_pair(tick, std::move(cell)));
    }
}

Tube::Insertion Tube::insert(const Climber& climber, Cell::PooledNode& cell)
{
    Insertion result;

    SpinGuard lock(m_spinner);

    const auto it(m_cells.find(climber.tick()));

    if (it != m_cells.end())
    {
        Cell::PooledNode& curr(it->second);

        if (cell->point() != curr->point())
        {
            const Point& center(climber.bounds().mid());

            if (cell->point().sqDist3d(center) < curr->point().sqDist3d(center))
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
            it->second->push(std::move(cell));
        }
    }
    else
    {
        result.setDone(cell->size());
        m_cells.emplace(std::make_pair(climber.tick(), std::move(cell)));
    }

    return result;
}

} // namespace entwine

