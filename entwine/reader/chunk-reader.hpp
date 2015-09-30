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
    friend class ChunkIter;

public:
    ChunkReader(
            const Schema& schema,
            const Id& id,
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
    const Id m_id;
    const std::size_t m_numPoints;

    std::vector<char> m_data;
    std::multimap<uint64_t, PointInfoShallow> m_points;
};

/*
class ChunkIter
{
public:
    ChunkIter(const ChunkReader& reader);

    bool next() { return ++m_index < m_numPoints; }

    const char* getData() const
    {
        return m_data.data() + m_pointSize * m_index;
    }

private:
    const std::vector<char>& m_data;

    std::size_t m_index;
    std::size_t m_numPoints;
    std::size_t m_pointSize;
};
*/

} // namespace entwine

