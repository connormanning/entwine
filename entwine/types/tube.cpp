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

bool Tube::insert(const Climber& climber, Cell::PooledNode& cell)
{
    SpinGuard lock(m_spinner);

    const auto it(m_cells.find(climber.tick()));

    if (it != m_cells.end())
    {
        Cell::PooledNode& curr(it->second);

        if (cell->point() != curr->point())
        {
            const Point& center(climber.bbox().mid());

            if (cell->point().sqDist3d(center) < curr->point().sqDist3d(center))
            {
                // Store our new cell, and send the previous one further down
                // the tree.
                std::swap(cell, curr);
            }

            return false;
        }
        else
        {
            it->second->push(std::move(cell));
            return true;
        }
    }
    else
    {
        m_cells.emplace(std::make_pair(climber.tick(), std::move(cell)));
        return true;
    }
}

} // namespace entwine

