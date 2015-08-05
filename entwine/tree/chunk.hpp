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

#include <array>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <entwine/types/dim-info.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/locker.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Schema;

class Entry
{
    friend class ContiguousChunkData;

public:
    Entry();
    Entry(char* data);
    Entry(const Point& point, char* data);

    Entry(const Entry& other);
    Entry& operator=(const Entry& other);

    Point point() const;
    const char* data() const;
    Locker getLocker();

    // Must fetch a Locker before modifying.
    void setPoint(const Point& point);
    void setData(char* pos);
    void update(const Point& point, const char* data, std::size_t size);

private:
    std::array<Point, 2> m_points;
    std::atomic_size_t m_active;

    std::atomic<Point*> m_atom;
    std::atomic_flag m_flag;
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
    ChunkData(const Schema& schema, const Id& id, std::size_t maxPoints);
    ChunkData(const ChunkData& other);
    virtual ~ChunkData();

    std::size_t maxPoints() const { return m_maxPoints; }
    const Id& id() const { return m_id; }

    virtual void save(arbiter::Endpoint& endpoint) = 0;

    virtual bool isSparse() const = 0;
    virtual std::size_t numPoints() const = 0;
    virtual Entry* getEntry(const Id& index) = 0;

protected:
    Id endId() const;
    std::size_t normalize(const Id& rawIndex) const;

    const Schema& m_schema;
    const Id m_id;
    const std::size_t m_maxPoints;
};

class SparseChunkData : public ChunkData
{
    friend class ContiguousChunkData;

public:
    SparseChunkData(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints);

    SparseChunkData(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& compressedData);

    ~SparseChunkData();

    virtual void save(arbiter::Endpoint& endpoint);

    virtual bool isSparse() const { return true; }
    virtual std::size_t numPoints() const { return m_entries.size(); }
    virtual Entry* getEntry(const Id& index);

    static std::size_t popNumPoints(std::vector<char>& compressedData);
    static DimList makeSparse(const Schema& schema);

private:
    struct SparseEntry
    {
        SparseEntry(const Schema& schema);
        SparseEntry(const Schema& schema, char* pos);

        Entry entry;
        std::vector<char> data;
    };

    std::mutex m_mutex;
    std::unordered_map<Id, std::unique_ptr<SparseEntry>> m_entries;

    // Creates a compact contiguous representation of this sparse chunk by
    // prepending an "EntryId" field to the native schema and inserting each
    // point from m_entries.
    std::vector<char> squash(const Schema& sparse);

    void pushNumPoints(std::vector<char>& data, std::size_t numPoints) const;
};

class ContiguousChunkData : public ChunkData
{
public:
    ContiguousChunkData(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            const std::vector<char>& empty);

    ContiguousChunkData(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& compressedData);

    ContiguousChunkData(
            SparseChunkData& sparse,
            const std::vector<char>& empty);

    ~ContiguousChunkData();

    void save(arbiter::Endpoint& endpoint, std::string postfix);
    virtual void save(arbiter::Endpoint& endpoint);

    virtual bool isSparse() const { return false; }
    virtual std::size_t numPoints() const { return m_maxPoints; }
    virtual Entry* getEntry(const Id& index);

    void merge(ContiguousChunkData& other);

private:
    void emptyEntries();

    std::vector<Entry> m_entries;
    std::unique_ptr<std::vector<char>> m_data;
};

class ChunkDataFactory
{
public:
    static std::unique_ptr<ChunkData> create(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& data);
};








class Chunk
{
public:
    Chunk(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            bool contiguous,
            const std::vector<char>& empty);

    Chunk(
            const Schema& schema,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char> data,
            const std::vector<char>& empty);

    Entry* getEntry(const Id& index);

    void save(arbiter::Endpoint& endpoint);

    static ChunkType getType(std::vector<char>& data);

    static std::size_t getChunkMem();
    static std::size_t getChunkCnt();

private:
    std::unique_ptr<ChunkData> m_chunkData;
    const double m_threshold;

    std::mutex m_mutex;
    std::atomic<bool> m_converting;

    const std::vector<char>& m_empty;
};

} // namespace entwine

