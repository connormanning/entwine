#include <cmath>
#include <cstdint>

#include "Epf.hpp"
#include "Grid.hpp"

using namespace pdal;

namespace epf
{

void Grid::expand(const BOX3D& bounds, size_t points)
{
    m_bounds.grow(bounds);
    m_millionPoints += (points / 1000000.0);

    //ABELL - Fix this for small point clouds.
    if (m_millionPoints >= 2000)
        m_gridSize = 64;
    else if (m_millionPoints >= 100)
        m_gridSize = 32;

    m_xsize = (m_bounds.maxx - m_bounds.minx) / m_gridSize;
    m_ysize = (m_bounds.maxy - m_bounds.miny) / m_gridSize;
    m_zsize = (m_bounds.maxz - m_bounds.minz) / m_gridSize;
}

int Grid::index(double x, double y, double z)
{
    int xi = std::floor((x - m_bounds.minx) / m_xsize);
    int yi = std::floor((y - m_bounds.miny) / m_ysize);
    int zi = std::floor((z - m_bounds.minz) / m_zsize);
    xi = (std::min)((std::max)(0, xi), m_gridSize - 1);
    yi = (std::min)((std::max)(0, yi), m_gridSize - 1);
    zi = (std::min)((std::max)(0, zi), m_gridSize - 1);

    return epf::toIndex(xi, yi, zi);
}

} // namespace epf
