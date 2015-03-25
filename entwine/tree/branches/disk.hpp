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
#include <cstdint>
#include <map>
#include <mutex>
#include <memory>

#include <entwine/tree/branch.hpp>

namespace entwine
{

class Chunk
{
public:
    Chunk(
            const std::string& path,
            std::size_t begin,
            const std::vector<char>& initData);

    bool addPoint(const PointInfo* toAdd);

    // TODO
    bool awaken() { return false; }
    bool sleep()  { return false; }

private:
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

    // Atomically determines whether the chunk has been created.
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
    ~DiskBranch();

    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual bool hasPoint(std::size_t index);
    virtual Point getPoint(std::size_t index);
    virtual std::vector<char> getPointData(
            std::size_t index,
            const Schema& schema);

private:
    // Returns the chunk ID that contains this index.
    std::size_t getChunkId(std::size_t index) const;

    virtual void saveImpl(const std::string& path, Json::Value& meta);

    const std::string& m_path;

    const std::size_t m_pointsPerChunk;
    std::vector<LockedChunk> m_chunks;

    const std::vector<char> m_emptyChunk;
};

} // namespace entwine

