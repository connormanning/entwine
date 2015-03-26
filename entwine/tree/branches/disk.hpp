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

#include <entwine/tree/branch.hpp>

namespace entwine
{

class Schema;

class Chunk
{
public:
    Chunk(
            const std::string& path,
            std::size_t begin,
            const std::vector<char>& initData);

    bool addPoint(
            const Schema& schema,
            PointInfo** toAddPtr,
            std::size_t offset);

    // TODO
    bool awaken() { return false; }
    bool sleep()  { return false; }

private:
    char* m_mapping;
};

class LockedChunk
{
public:
    ~LockedChunk();

    // Initialize the chunk.  Thread-safe and idempotent.
    void init(
            const std::string& path,
            std::size_t begin,
            const std::vector<char>& data);

    Chunk& get() { return *m_chunk.load(); }

    // If this returns false, init() must be called before using get().
    bool exists() const;

private:
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
    // Returns the chunk ID that contains this index.
    std::size_t getChunkId(std::size_t index) const;

    // Returns the byte offset for this index within the specified chunkId.
    std::size_t getByteOffset(std::size_t chunkId, std::size_t index) const;

    virtual void saveImpl(const std::string& path, Json::Value& meta);

    const std::string& m_path;

    const std::size_t m_pointsPerChunk;
    std::vector<LockedChunk> m_chunks;

    const std::vector<char> m_emptyChunk;
};

} // namespace entwine

