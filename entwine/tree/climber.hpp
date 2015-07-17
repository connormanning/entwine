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

#include <entwine/types/bbox.hpp>

namespace entwine
{

class Point;
class Structure;

// Maintains the state of the current point as it traverses the virtual tree.
class Climber
{
public:
    Climber(const BBox& bbox, const Structure& structure);

    void magnify(const Point& point);
    std::size_t index() const;
    std::size_t depth() const;
    const BBox& bbox() const;

    void goNw();
    void goNe();
    void goSw();
    void goSe();

    // TODO Octree support.
    void goNwu();
    void goNwd();
    void goNeu();
    void goNed();
    void goSwu();
    void goSwd();
    void goSeu();
    void goSed();

    Climber getNw() const;
    Climber getNe() const;
    Climber getSw() const;
    Climber getSe() const;

    enum Dir
    {
        nw = 0,
        ne = 1,
        sw = 2,
        se = 3
    };

private:
    const Structure& m_structure;
    std::size_t m_dimensions;

    std::size_t m_index;
    std::size_t m_depth;

    BBox m_bbox;

    void step(Dir dir);
};

} // namespace entwine

