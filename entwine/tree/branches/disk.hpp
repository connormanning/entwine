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
    bool addPoint(const PointInfo* toAdd);

private:
};

class LockedChunk
{
public:
    Chunk& get()
    {
        if (!m_chunk.load())
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_chunk.load())
            {
                m_chunk.store(new Chunk());
            }
        }

        return *m_chunk.load();
    }

private:
    std::mutex m_mutex;
    std::atomic<Chunk*> m_chunk;
};

class DiskBranch : public Branch
{
public:
    DiskBranch(
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
    virtual const Point* getPoint(std::size_t index);
    virtual std::vector<char> getPointData(
            std::size_t index,
            const Schema& schema);

private:
    // Returns the chunk ID that contains this index.
    std::size_t getChunkId(std::size_t index) const;

    virtual void saveImpl(const std::string& path, Json::Value& meta);

    std::vector<LockedChunk> m_chunks;
    const std::size_t m_chunkSize;
};

} // namespace entwine

