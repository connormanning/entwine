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
#include <vector>

#include <entwine/reader/reader.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

class Block;
class Schema;

class Query
{
    friend class Reader;

public:
    Query(
            Reader& reader,
            const Schema& outSchema,
            std::unique_ptr<Block> block);

    // Returns the number of points in the query result.
    std::size_t size() const;

    // Get point data at the specified index.  Throws std::out_of_range if
    // index is greater than or equal to Query::size().
    //
    // These operations are not thread-safe.
    void get(std::size_t index, char* out) const;
    std::vector<char> get(std::size_t index) const;

private:
    // For use by the Reader when populating this Query.
    void insert(const char* pos);
    Point unwrapPoint(const char* pos) const;
    const ChunkMap& chunkMap() const { return m_block->chunkMap(); }

    Reader& m_reader;
    const Schema& m_outSchema;

    std::unique_ptr<Block> m_block;
    std::vector<const char*> m_points;

    mutable SinglePointTable m_table;
    LinkingPointView m_view;
};

} // namespace entwine

