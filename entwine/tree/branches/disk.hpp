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
    class PointMapper;
}

class Schema;

// A 'chunk' is a pre-allocated file of constant size, which is managed by this
// structure.  Each chunk is further broken down into dynamically created
// 'slots', which are equally-sized fractions of the full chunk.  Slots are
// managed by the PointMapper.
//
// This class takes care of:
//    - Writing the initial file when an addPoint call requires a chunk that
//      does not yet exist.
//    - Initializing the PointMapper on this chunk once it's created.
//    - Providing safe access to the PointMapper.
class ChunkManager
{
public:
    ChunkManager(
            const std::string& path,
            const Schema& schema,
            std::size_t begin,
            std::size_t chunkSize);

    ~ChunkManager();

    // Initialize the mapper, creating its backing file if needed.  Thread-safe
    // and idempotent.  This is only necessary in order to write to the backing
    // file.  For read-only queries, just call get(), which will only create
    // the PointMapper if the backing file already exists and the mapper is
    // not live.
    //
    // Returns true if the backing file was created and written, or false if
    // it already existed.  A false return value does not mean that get() will
    // fail.
    bool create(const std::vector<char>& initData);

    // Initialize the mapper in a read-only fashion, returning a null pointer
    // if the backing file does not exist.
    fs::PointMapper* getMapper();

    // True if our owned Mapper has been initialized.
    bool live() const;

    std::size_t id() const { return m_begin; }

private:
    const std::string m_filename;
    const Schema& m_schema;
    const std::size_t m_begin;
    const std::size_t m_chunkSize;

    std::mutex m_mutex;
    std::atomic<fs::PointMapper*> m_mapper;
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

    virtual void grow(Clipper* clipper, std::size_t index);
    virtual void clip(Clipper* clipper, std::size_t index);

private:
    void initChunkManagers();

    ChunkManager& getChunkManager(std::size_t index);

    virtual void saveImpl(const std::string& path, Json::Value& meta);

    const std::string& m_path;

    std::set<std::size_t> m_ids;

    const std::size_t m_pointsPerChunk;
    std::vector<std::unique_ptr<ChunkManager>> m_chunkManagers;

    const std::vector<char> m_emptyChunk;
};

} // namespace entwine

