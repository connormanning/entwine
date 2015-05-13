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
public:
    Entry(char* data);
    Entry(const Point* point, char* data);
    ~Entry();

    std::atomic<const Point*>& point() { return m_point.atom; }
    std::mutex& mutex() { return m_mutex; }
    char* data() { return m_data; } // Must hold lock on mutex for access.

private:
    void setData(char* pos) { }     // TODO

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

    virtual bool isSparse() const = 0;
    virtual std::size_t numPoints() const { return m_maxPoints; }

    virtual Entry& getEntry(std::size_t rawIndex) = 0;

    virtual void save(Source& source) = 0;

    virtual void finalize(
            Source& source,
            std::vector<std::size_t>& ids,
            std::mutex& idsMutex,
            const std::size_t start,
            const std::size_t chunkPoints) = 0;

protected:
    std::size_t normalize(std::size_t rawIndex);
    std::size_t endId() const;

    const Schema& m_schema;
    const std::size_t m_id;
    const std::size_t m_maxPoints;
};

/*
class SparseChunkData : public ChunkData
{
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

    virtual void save(Source& source);

    virtual void finalize(
            Source& source,
            std::vector<std::size_t>& ids,
            std::mutex& idsMutex,
            const std::size_t start,
            const std::size_t chunkPoints);

    std::map<std::size_t, SparseEntry>& entries() { return m_entries; }
    std::mutex& mutex() { return m_mutex; }

private:
    std::map<std::size_t, Entry> m_entries;
    std::map<std::size_t, std::vector<char>> m_data;

    void write(Source& source, std::size_t begin, std::size_t end);
    std::vector<char> squash(
            const Schema& sparse,
            std::size_t begin,
            std::size_t end);
};
*/

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

    // ContiguousChunkData(const SparseChunkData& other);

    virtual bool isSparse() const { return false; }

    virtual Entry& getEntry(std::size_t rawIndex);

    virtual void save(Source& source);

    virtual void finalize(
            Source& source,
            std::vector<std::size_t>& ids,
            std::mutex& idsMutex,
            const std::size_t start,
            const std::size_t chunkPoints);

private:
    void write(
            Source& source,
            std::size_t begin,  // This corresponds with the name of the chunk.
            std::size_t end);

    void makeEmpty();

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
};

} // namespace entwine

