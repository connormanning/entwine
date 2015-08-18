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

#include <entwine/types/structure.hpp>
#include <entwine/tree/chunk.hpp>

namespace entwine
{

class Schema;

class ChunkReader
{
    friend class ChunkIter;

public:
    virtual ~ChunkReader() { }
    static std::unique_ptr<ChunkReader> create(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const = 0;
    virtual const char* getData(const Id& rawIndex) const = 0;

protected:
    virtual const std::vector<char>& getRaw() const = 0;
    virtual std::size_t numPoints() const = 0;

    ChunkReader(const Schema& schema, const Id& id, std::size_t maxPoints);

    std::size_t normalize(const Id& rawIndex) const
    {
        return (rawIndex - m_id).getSimple();
    }

    const Schema& m_schema;
    const Id m_id;
    const std::size_t m_maxPoints;
};

class SparseReader : public ChunkReader
{
public:
    SparseReader(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const { return true; }
    virtual const char* getData(const Id& rawIndex) const;

private:
    virtual const std::vector<char>& getRaw() const { return m_raw; }
    virtual std::size_t numPoints() const { return m_ids.size(); }

    std::vector<char> m_raw;
    std::map<std::size_t, char*> m_ids;
};

class ContiguousReader : public ChunkReader
{
public:
    ContiguousReader(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const { return false; }
    virtual const char* getData(const Id& rawIndex) const;

private:
    virtual const std::vector<char>& getRaw() const { return *m_data; }
    virtual std::size_t numPoints() const { return m_maxPoints; }

    std::unique_ptr<std::vector<char>> m_data;
};

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

} // namespace entwine

