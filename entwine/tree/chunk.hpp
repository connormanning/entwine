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
#include <entwine/util/locker.hpp>

namespace entwine
{

class Schema;
class Source;

class Entry
{
    friend class ContiguousChunkData;

public:
    Entry();
    Entry(char* data);
    Entry(const Point& point, char* data);

    Entry(const Entry& other);
    Entry& operator=(const Entry& other);

    Point getPoint() const;
    Locker getLocker();

    // Must fetch a Locker before calling setPoint() or modifying data.
    void setPoint(const Point& point);
    char* data();

    void setData(char* pos) { m_data = pos; }

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
    ChunkData(const Schema& schema, std::size_t id, std::size_t maxPoints);
    ChunkData(const ChunkData& other);
    virtual ~ChunkData();

    std::size_t maxPoints() const { return m_maxPoints; }
    std::size_t id() const { return m_id; }

    virtual void save(Source& source) = 0;

    virtual bool isSparse() const = 0;
    virtual std::size_t numPoints() const = 0;
    virtual Entry* getEntry(std::size_t index) = 0;

protected:
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

    ~SparseChunkData();

    virtual void save(Source& source);

    virtual bool isSparse() const { return true; }
    virtual std::size_t numPoints() const { return m_entries.size(); }
    virtual Entry* getEntry(std::size_t index);

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
    std::unordered_map<std::size_t, std::unique_ptr<SparseEntry>> m_entries;

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
            std::size_t id,
            std::size_t maxPoints,
            const std::vector<char>& empty);

    ContiguousChunkData(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char>& compressedData);

    ContiguousChunkData(
            SparseChunkData& sparse,
            const std::vector<char>& empty);

    ~ContiguousChunkData();

    void save(Source& source, std::string postfix);
    virtual void save(Source& source);

    virtual bool isSparse() const { return false; }
    virtual std::size_t numPoints() const { return m_maxPoints; }
    virtual Entry* getEntry(std::size_t index);

    void merge(ContiguousChunkData& other);

private:
    void emptyEntries();
    std::size_t normalize(std::size_t rawIndex);

    std::vector<Entry> m_entries;
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
            std::size_t maxPoints,
            bool contiguous,
            const std::vector<char>& empty);

    Chunk(
            const Schema& schema,
            std::size_t id,
            std::size_t maxPoints,
            std::vector<char> data,
            const std::vector<char>& empty);

    Entry* getEntry(std::size_t index);

    void save(Source& source);

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

