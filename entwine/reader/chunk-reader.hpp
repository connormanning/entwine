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
public:
    static std::unique_ptr<ChunkReader> create(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const = 0;
    virtual const char* getData(const Id& rawIndex) const = 0;

protected:
    ChunkReader(const Schema& schema, const Id& id, std::size_t maxPoints);

    const Schema& m_schema;
    const Id m_id;
    const std::size_t m_maxPoints;
};

class SparseReader : public ChunkReader
{
    friend class SparseIter;

public:
    SparseReader(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const { return true; }
    virtual const char* getData(const Id& rawIndex) const;

private:
    std::map<Id, std::vector<char>> m_data;
};

class ContiguousReader : public ChunkReader
{
    friend class ContiguousIter;

public:
    ContiguousReader(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::unique_ptr<std::vector<char>> data);

    virtual bool sparse() const { return false; }
    virtual const char* getData(const Id& rawIndex) const;

private:
    std::unique_ptr<std::vector<char>> m_data;
};

class ChunkIter
{
public:
    virtual ~ChunkIter() { }
    static std::unique_ptr<ChunkIter> create(const ChunkReader& chunkReader);

    virtual bool next() = 0;
    virtual const char* getData() const = 0;
};

class SparseIter : public ChunkIter
{
public:
    SparseIter(const SparseReader& reader)
        : m_cur(reader.m_data.begin())
        , m_end(reader.m_data.end())
    { }

    virtual bool next() { return ++m_cur != m_end; }
    virtual const char* getData() const { return m_cur->second.data(); }

private:
    std::map<Id, std::vector<char>>::const_iterator m_cur;
    std::map<Id, std::vector<char>>::const_iterator m_end;
};

class ContiguousIter : public ChunkIter
{
public:
    ContiguousIter(const ContiguousReader& reader);

    virtual bool next() { return ++m_index < m_maxPoints; }
    virtual const char* getData() const
    {
        return m_data.data() + m_pointSize * m_index;
    }

private:
    std::size_t m_index;
    std::size_t m_maxPoints;
    const std::vector<char>& m_data;
    std::size_t m_pointSize;
};

} // namespace entwine

