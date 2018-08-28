/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cassert>
#include <cstddef>
#include <forward_list>
#include <mutex>
#include <stack>
#include <unordered_map>
#include <utility>

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

#include <entwine/util/spin-lock.hpp>

namespace entwine
{

struct CountedCells
{
    CountedCells(Cell::Pool& pool) : stack(pool) { }
    Cells stack;
    uint64_t np = 0;
};

class Chunk;

class ReffedChunk
{
public:
    ReffedChunk(
            const ChunkKey& key,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            Hierarchy& hierarchy);

    ReffedChunk(const ReffedChunk& o);
    ~ReffedChunk();

    struct Info
    {
        std::size_t written = 0;
        std::size_t read = 0;
        void clear() { written = 0; read = 0; }
    };

    bool insert(Cell::PooledNode& cell, const Key& key, Clipper& clipper);

    void ref(Clipper& clipper);
    void unref(Origin o);
    bool empty();

    Chunk& chunk() { assert(m_chunk); return *m_chunk; }

    const ChunkKey& key() const { return m_key; }
    const Metadata& metadata() const { return m_metadata; }
    const arbiter::Endpoint& out() const { return m_out; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }
    PointPool& pointPool() const { return m_pointPool; }
    Hierarchy& hierarchy() const { return m_hierarchy; }

    static Info latchInfo();

private:
    ChunkKey m_key;
    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;
    Hierarchy& m_hierarchy;

    // std::mutex m_mutex;
    SpinLock m_mutex;
    std::unique_ptr<Chunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

class Voxel
{
public:
    bool insert(const Point& p, const char* data)
    {
        if (m_list.empty() || m_point == p)
        {
            m_point = p;
            m_list.push_front(data);
            // m_data.insert(m_data.end(), data, data + pointSize);
            return true;
        }
        return false;
    }

private:
    Point m_point;
    // std::vector<char> m_data;
    std::forward_list<const char*> m_list;
};

struct VoxelTube
{
    VoxelTube() { }

    // std::mutex mutex;
    SpinLock mutex;
    std::map<uint64_t, Voxel> map;
};

class Chunk
{
public:
    Chunk(const ReffedChunk& ref)
        : m_ref(ref)
        , m_overflow(m_ref.pointPool().cellPool())
        , m_ticks(m_ref.metadata().ticks())
        , m_dataPool(m_ref.pointPool().schema().pointSize(), 1024)
        , m_stack(m_dataPool)
    {
        init();

        m_children.reserve(dirEnd());
        for (std::size_t d(0); d < dirEnd(); ++d)
        {
            const ChunkKey key(m_ref.key().getStep(toDir(d)));
            m_children.emplace_back(
                    key,
                    m_ref.out(),
                    m_ref.tmp(),
                    m_ref.pointPool(),
                    m_ref.hierarchy());

            m_hasChildren = m_hasChildren || m_ref.hierarchy().get(key.get());
        }
    }

    void init()
    {
        assert(!m_keys);
        assert(m_overflow.empty());
        assert(!m_overflowCount);
        m_grid = makeUnique<std::vector<VoxelTube>>(m_ticks * m_ticks);
        m_keys = makeUnique<std::vector<Key>>();
        m_remote = false;
    }

    ReffedChunk& step(const Point& p)
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return m_children[toIntegral(dir)];
    }

    bool terminus()
    {
        // Make sure we don't early-return here - need to traverse all children.
        bool result(true);
        for (auto& c : m_children) if (!c.empty()) result = false;
        return result;
    }

    Data::PooledStack acquireBinary()
    {
        m_grid.reset();
        m_keys.reset();

        if (m_overflowCount)
        {
            assert(!m_overflow.empty());
            assert(!m_hasChildren);

            for (auto& cell : m_overflow)
            {
                m_stack.push(cell.acquire());
            }

            m_overflowCount = 0;
        }

        m_grid.reset();
        m_keys.reset();

        m_remote = true;

        return std::move(m_stack);
    }

    bool remote() const { return m_remote; }

    bool insert(const Key& key, Cell::PooledNode& cell, Clipper& clipper)
    {
        if (insertNative(key, cell))
        {
            return true;
        }

        if (m_ref.key().depth() >= m_ref.metadata().overflowDepth())
        {
            return insertOverflow(key, cell, clipper);
        }

        return false;
    }

private:
    bool insertNative(const Key& key, Cell::PooledNode& cell)
    {
        const Xyz& pos(key.position());
        const std::size_t i((pos.y % m_ticks) * m_ticks + (pos.x % m_ticks));

        VoxelTube& tube((*m_grid)[i]);

        // std::lock_guard<std::mutex> tubeLock(tube.mutex);
        SpinGuard tubeLock(tube.mutex);
        Voxel& voxel(tube.map[pos.z]);

        const char* data(cell->uniqueData());

        if (voxel.insert(cell->point(), data))
        {
            // std::lock_guard<std::mutex> lock(m_mutex);
            SpinGuard lock(m_mutex);

            Data::PooledNode node(m_dataPool.acquireOne());
            std::copy(data, data + m_dataPool.bufferSize(), *node);
            m_stack.push(std::move(node));
            return true;
        }
        return false;
    }

    bool insertOverflow(
            const Key& key,
            Cell::PooledNode& cell,
            Clipper& clipper)
    {
        // std::lock_guard<std::mutex> lock(m_overflowMutex);
        SpinGuard lock(m_overflowMutex);
        if (m_hasChildren) return false;

        assert(m_keys);
        m_overflowCount += cell->size();
        m_overflow.push(std::move(cell));
        m_keys->push_back(key);

        assert(m_overflowCount >= m_overflow.size());
        assert(m_overflow.size() == m_keys->size());

        if (m_overflowCount > m_ref.metadata().overflowThreshold())
        {
            doOverflow(clipper);
        }

        return true;
    }

    void doOverflow(Clipper& clipper);

    const ReffedChunk& m_ref;
    bool m_remote = false;

    // std::mutex m_overflowMutex;
    SpinLock m_overflowMutex;
    bool m_hasChildren = false;
    uint64_t m_overflowCount = 0;
    Cell::PooledStack m_overflow;
    std::unique_ptr<std::vector<Key>> m_keys;

    const uint64_t m_ticks;




    // mutable std::mutex m_mutex;
    SpinLock m_mutex;
    Data::Pool m_dataPool;
    Data::PooledStack m_stack;
    std::unique_ptr<std::vector<VoxelTube>> m_grid;

    std::vector<ReffedChunk> m_children;
};

} // namespace entwine

