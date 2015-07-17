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

    void goNwd();
    void goNed();
    void goSwd();
    void goSed();
    void goNwu();
    void goNeu();
    void goSwu();
    void goSeu();

    Climber getNwd() const;
    Climber getNed() const;
    Climber getSwd() const;
    Climber getSed() const;
    Climber getNwu() const;
    Climber getNeu() const;
    Climber getSwu() const;
    Climber getSeu() const;

    enum Dir
    {
        nwd = 0,
        ned = 1,
        swd = 2,
        sed = 3,
        nwu = 4,
        neu = 5,
        swu = 6,
        seu = 7,
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

