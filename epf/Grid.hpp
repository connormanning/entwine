#pragma once

#include <pdal/util/Bounds.hpp>

namespace epf
{

class Grid
{
public:
    Grid() : m_gridSize(16), m_millionPoints(0)
    {}

    void expand(const pdal::BOX3D& bounds, size_t points);
    int index(double x, double y, double z);

private:
    int m_gridSize;
    pdal::BOX3D m_bounds;
    size_t m_millionPoints;
    double m_xsize;
    double m_ysize;
    double m_zsize;
};

}
