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

#include <entwine/tree/chunk.hpp>

namespace entwine
{

class Schema;

class ChunkReader
{
public:
    static std::unique_ptr<ChunkReader> create(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual const char* getData(std::size_t rawIndex) const = 0;

protected:
    ChunkReader(const Schema& schema, std::size_t id, std::size_t maxPoints);

    const Schema& m_schema;
    const std::size_t m_id;
    const std::size_t m_maxPoints;
};

class SparseReader : public ChunkReader
{
public:
    SparseReader(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual const char* getData(std::size_t rawIndex) const;

private:
    std::map<std::size_t, std::vector<char>> m_data;
};

class ContiguousReader : public ChunkReader
{
public:
    ContiguousReader(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual const char* getData(std::size_t rawIndex) const;

private:
    std::unique_ptr<std::vector<char>> m_data;
};

} // namespace entwine

