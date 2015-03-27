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
#include <cstdint>
#include <map>
#include <mutex>
#include <memory>
#include <set>

#include <entwine/tree/branch.hpp>

namespace entwine
{

namespace fs
{
    class FileDescriptor;
}

class Schema;

class Chunk
{
public:
    Chunk(
            const Schema& schema,
            const std::string& path,
            std::size_t begin,
            const std::vector<char>& initData);
    Chunk(
            const Schema& schema,
            const std::string& path,
            std::size_t begin,
            const std::size_t chunkSize);
    ~Chunk();

    bool addPoint(const Roller& roller, PointInfo** toAddPtr);

    bool hasPoint(std::size_t index) const;
    Point getPoint(std::size_t index) const;
    std::vector<char> getPointData(std::size_t index) const;

    void sync();

private:
    std::size_t getByteOffset(std::size_t index) const;

    const Schema& m_schema;
    std::unique_ptr<fs::FileDescriptor> m_fd;
    char* m_mapping;
    const std::size_t m_begin;
    const std::size_t m_size;
    std::mutex m_mutex;
};

class LockedChunk
{
public:
    LockedChunk(std::size_t begin);
    ~LockedChunk();

    // Initialize the chunk, creating its backing file if needed.  Thread-safe
    // and idempotent.
    Chunk* init(
            const Schema& schema,
            const std::string& path,
            const std::vector<char>& data);

    // Read-only version to awaken the chunk for querying.  Returns null
    // pointer if there is no file for this chunk.
    Chunk* awaken(
            const Schema& schema,
            const std::string& path,
            std::size_t chunkSize);

    Chunk* get() { return m_chunk.load(); }

    // True if the underlying file for this chunk exists.
    bool backed(const std::string& path) const;

    // True if our owned Chunk has been initialized.
    bool live() const;

    std::size_t id() const { return m_begin; }

private:
    const std::size_t m_begin;
    std::mutex m_mutex;
    std::atomic<Chunk*> m_chunk;
};

class DiskBranch : public Branch
{
public:
    DiskBranch(
            const std::string& path,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthBegin,
            std::size_t depthEnd);
    DiskBranch(
            const std::string& path,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);

    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual bool hasPoint(std::size_t index);
    virtual Point getPoint(std::size_t index);
    virtual std::vector<char> getPointData(std::size_t index);

private:
    LockedChunk& getLockedChunk(std::size_t index);
    std::size_t getChunkIndex(std::size_t index) const;

    virtual void saveImpl(const std::string& path, Json::Value& meta);

    const std::string& m_path;

    std::set<std::size_t> m_ids;
    std::mutex m_mutex;

    const std::size_t m_pointsPerChunk;
    std::vector<std::unique_ptr<LockedChunk>> m_chunks;

    const std::vector<char> m_emptyChunk;
};

} // namespace entwine

