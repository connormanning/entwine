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
#include <map>
#include <memory>
#include <vector>

#include <entwine/tree/point-info.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Schema;

class ChunkReader
{
public:
    ChunkReader(
            const Schema& schema,
            const BBox& bbox,
            const Id& id,
            std::size_t depth,
            std::unique_ptr<std::vector<char>> data);

    std::size_t query(
            std::vector<char>& buffer,
            const Schema& outSchema,
            const BBox& qbox) const;

private:
    std::size_t numPoints() const { return m_points.size(); }
    const Schema& schema() const { return m_schema; }

    std::size_t normalize(const Id& rawIndex) const
    {
        return (rawIndex - m_id).getSimple();
    }

    const Schema& m_schema;
    const BBox& m_bbox;
    const Id m_id;
    const std::size_t m_depth;
    const std::size_t m_numPoints;

    std::unique_ptr<std::vector<char>> m_data;
    std::multimap<uint64_t, PointInfoNonPooled> m_points;
};

} // namespace entwine

