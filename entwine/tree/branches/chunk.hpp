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

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include <entwine/types/elastic-atomic.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

class Schema;
class Source;

class Entry
{
    friend class ContiguousChunkData;

public:
    Entry(char* data);
    Entry(const Point* point, char* data);
    ~Entry();

    std::atomic<const Point*>& point() { return m_point.atom; }
    std::mutex& mutex() { return m_mutex; }
    char* data() { return m_data; } // Must hold lock on mutex for access.

private:
    void setData(char* pos) { m_data = pos; }

    ElasticAtomic<const Point*> m_point;
    std::mutex m_mutex;
    char* m_data;
};

enum ChunkType
{
    Sparse = 0,
    Contiguous
};


class ChunkData
{
public:
    ChunkData(const Schema& schema, std::size_t id, std::size_t maxPoints);
    ChunkData(const ChunkData& other);
    virtual ~ChunkData();

    std::size_t maxPoints() const { return m_maxPoints; }

    void save(Source& source);

    void finalize(
            Source& source,
            std::vector<std::size_t>& ids,
            std::mutex& idsMutex,
            const std::size_t start,
            const std::size_t chunkPoints);

    virtual bool isSparse() const = 0;
    virtual std::size_t numPoints() const = 0;
    virtual Entry& getEntry(std::size_t rawIndex) = 0;

protected:
    virtual void write(Source& source, std::size_t begin, std::size_t end) = 0;

    std::size_t endId() const;

    const Schema& m_schema;
    const std::size_t m_id;
    const std::size_t m_maxPoints;
};

class SparseChunkData : public ChunkData
{
    friend class ContiguousChunkData;

public:
    SparseChunkData(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints);

    SparseChunkData(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char>& compressedData);

    virtual bool isSparse() const { return true; }
    virtual std::size_t numPoints() const { return m_entries.size(); }
    virtual Entry& getEntry(std::size_t rawIndex);

    static std::size_t popNumPoints(std::vector<char>& compressedData);

private:
    virtual void write(Source& source, std::size_t begin, std::size_t end);

    struct SparseEntry
    {
        SparseEntry(const Schema& schema);
        SparseEntry(const Schema& schema, char* pos);

        std::vector<char> data;
        std::unique_ptr<Entry> entry;
    };

    std::mutex m_mutex;
    std::map<std::size_t, SparseEntry> m_entries;

    // Creates a compact contiguous representation of this sparse chunk by
    // prepending an "EntryId" field to the native schema and inserting each
    // point from m_entries.
    std::vector<char> squash(
            const Schema& sparse,
            std::size_t begin,
            std::size_t end);

    void pushNumPoints(std::vector<char>& data, std::size_t numPoints) const;
};

class ContiguousChunkData : public ChunkData
{
public:
    ContiguousChunkData(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints);

    ContiguousChunkData(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char>& compressedData);

    ContiguousChunkData(SparseChunkData& sparse);

    virtual bool isSparse() const { return false; }
    virtual std::size_t numPoints() const { return m_maxPoints; }
    virtual Entry& getEntry(std::size_t rawIndex);

private:
    virtual void write(Source& source, std::size_t begin, std::size_t end);

    void makeEmpty();
    std::size_t normalize(std::size_t rawIndex);

    std::vector<std::unique_ptr<Entry>> m_entries;
    std::unique_ptr<std::vector<char>> m_data;
};

class ChunkDataFactory
{
public:
    static std::unique_ptr<ChunkData> create(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char>& data);
};








class Chunk
{
public:
    Chunk(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints);

    Chunk(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char> data);

    Entry& getEntry(std::size_t rawIndex);

    void save(Source& source);

    void finalize(
            Source& source,
            std::vector<std::size_t>& ids,
            std::mutex& idsMutex,
            std::size_t start,
            std::size_t chunkPoints);

private:
    std::unique_ptr<ChunkData> m_chunkData;
    const double m_threshold;

    std::mutex m_mutex;
    bool m_converting;
};











class ChunkReader
{
public:
    static std::unique_ptr<ChunkReader> create(
            std::size_t id,
            const Schema& schema,
            std::unique_ptr<std::vector<char>> data);

    virtual char* getData(std::size_t rawIndex) = 0;

protected:
    ChunkReader(std::size_t id, const Schema& schema);

    const std::size_t m_id;
    const Schema& m_schema;
};

class SparseReader : public ChunkReader
{
public:
    SparseReader(
            std::size_t id,
            const Schema& schema,
            std::unique_ptr<std::vector<char>> data);

    virtual char* getData(std::size_t rawIndex);

private:
    std::map<std::size_t, std::vector<char>> m_data;
};

class ContiguousReader : public ChunkReader
{
public:
    ContiguousReader(
            std::size_t id,
            const Schema& schema,
            std::unique_ptr<std::vector<char>> data);

    virtual char* getData(std::size_t rawIndex);

private:
    std::unique_ptr<std::vector<char>> m_data;
};

} // namespace entwine

