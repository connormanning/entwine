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
#include <memory>
#include <mutex>
#include <set>

#include <entwine/tree/branch.hpp>

namespace entwine
{

class Chunk;
struct Entry;
class Schema;

class ColdBranch : public Branch
{
public:
    ColdBranch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t chunkPoints,
            std::size_t depthBegin,
            std::size_t depthEnd);

    ColdBranch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t chunkPoints,
            const Json::Value& meta);

    ~ColdBranch();

private:
    std::size_t getChunkId(std::size_t index) const;    // Global point index.
    std::size_t getSlotId(std::size_t chunkId) const;   // Local zero-based.
    std::unique_ptr<std::vector<char>> fetch(std::size_t chunkId) const;

    virtual std::unique_ptr<Entry> getEntry(std::size_t index);

    virtual void finalizeImpl(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize);

    virtual void grow(Clipper* clipper, std::size_t index);
    virtual void clip(Clipper* clipper, std::size_t index);

    //

    const std::size_t m_chunkPoints;

    struct ChunkInfo
    {
        std::unique_ptr<Chunk> chunk;
        std::set<const Clipper*> refs;
        std::mutex mutex;
    };

    std::mutex m_mutex;
    std::map<std::size_t, ChunkInfo> m_chunks;
};

} // namespace entwine

