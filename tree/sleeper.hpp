/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cstdint>
#include <memory>

#include "types/bbox.hpp"
#include "tree/registry.hpp"

struct PointInfo;

class Sleeper
{
public:
    Sleeper(const BBox& bbox, std::size_t pointSize);
    Sleeper(
            const BBox& bbox,
            std::size_t pointSize,
            std::shared_ptr<std::vector<char>> data);

    void addPoint(PointInfo** toAddPtr);

    void getPoints(
            MultiResults& results,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void getPoints(
            MultiResults& results,
            const BBox& query,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::shared_ptr<std::vector<char>> baseData();

    BBox bbox() const;

private:
    const BBox m_bbox;

    Registry m_registry;
};

