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

#include <entwine/types/vector-point-table.hpp>
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

    // bool insert(Cell::PooledNode& cell, const Key& key, Clipper& clipper);
    void insert(Voxel& voxel, Key& key, Clipper& clipper);

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

    SpinLock m_spin;
    std::unique_ptr<Chunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

class ShallowVoxel
{
public:
    bool insert(const Point& p, const char* data)
    {
        if (m_list.empty() || m_point == p)
        {
            m_point = p;
            m_list.push_front(data);
            return true;
        }
        return false;
    }

private:
    Point m_point;
    std::forward_list<const char*> m_list;
};

struct ShallowVoxelTube
{
    ShallowVoxelTube() { }

    SpinLock spin;
    std::map<uint64_t, ShallowVoxel> map;
};

struct VoxelTube
{
    SpinLock spin;
    std::map<uint64_t, Voxel> map;
};

class Chunk
{
public:
    Chunk(const ReffedChunk& ref)
        : m_ref(ref)
        , m_ticks(m_ref.metadata().ticks())
        , m_overflowPool(m_ref.metadata().schema().pointSize(), 1024)
        , m_overflowStack(m_overflowPool)
        // , m_overflow(m_ref.pointPool().cellPool())
        , m_dataPool(m_ref.metadata().schema().pointSize(), 4096)
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
        // assert(m_overflow.empty());
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

    Data::RawStack acquireBinary()
    {
        m_grid.reset();
        m_keys.reset();

        m_remote = true;

        Data::RawStack stack(m_stack.stack());
        stack.push(m_overflowStack.stack());
        return stack;
    }

    bool remote() const { return m_remote; }

    void insert(Voxel& voxel, Key& key, Clipper& clipper)
    {
        if (!insertNative(voxel, key) && !insertOverflow(voxel, key, clipper))
        {
            const auto dir(getDirection(key.bounds().mid(), voxel.point()));
            key.step(dir);
            m_children[toIntegral(dir)].insert(voxel, key, clipper);
        }
    }

private:
    bool insertNative(Voxel& voxel, Key& key)
    {
        const Xyz& pos(key.position());
        const std::size_t i((pos.y % m_ticks) * m_ticks + (pos.x % m_ticks));
        VoxelTube& tube((*m_grid)[i]);

        UniqueSpin tubeLock(tube.spin);
        Voxel& current(tube.map[pos.z]);

        if (current.accepts(voxel))
        {
            Data::PooledNode node(m_dataPool.acquireOne());
            const char* src(**voxel.stack().head());
            std::copy(src, src + m_ref.metadata().schema().pointSize(), *node);
            current.stack().push(node.get());
            return true;
        }
        return false;
    }

    /*
    bool insertNative(const Key& key, Cell::PooledNode& cell)
    {
        const Xyz& pos(key.position());
        const std::size_t i((pos.y % m_ticks) * m_ticks + (pos.x % m_ticks));

        ShallowVoxelTube& tube((*m_grid)[i]);

        UniqueSpin tubeLock(tube.spin);
        ShallowVoxel& voxel(tube.map[pos.z]);
        // tubeLock.unlock();

        const char* data(cell->uniqueData());

        if (voxel.insert(cell->point(), data))
        {
            SpinGuard lock(m_spin);

            Data::PooledNode node(m_dataPool.acquireOne());
            std::copy(data, data + m_dataPool.bufferSize(), *node);
            m_stack.push(std::move(node));
            return true;
        }
        return false;
    }
    */

    bool insertOverflow(Voxel& voxel, Key& key, Clipper& clipper)
    {
        if (m_ref.key().depth() < m_ref.metadata().overflowDepth())
        {
            return false;
        }

        SpinGuard lock(m_overflowSpin);
        if (m_hasChildren) return false;

        assert(m_keys);
        Data::PooledNode node(m_overflowPool.acquireOne());
        const char* src(**voxel.stack().head());
        std::copy(src, src + m_ref.metadata().schema().pointSize(), *node);

        Voxel v;
        v.point() = voxel.point();
        v.stack().push(node.get());

        m_overflowStack.push(std::move(node));
        m_overflow.push_back(v);
        m_keys->push_back(key);

        if (m_overflowStack.size() > m_ref.metadata().overflowThreshold())
        {
            doOverflow(clipper);
        }

        return true;
    }

    /*
    bool insertOverflow(
            const Key& key,
            Cell::PooledNode& cell,
            Clipper& clipper)
    {
        SpinGuard lock(m_overflowSpin);
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
    */

    void doOverflow(Clipper& clipper);

    const ReffedChunk& m_ref;
    bool m_remote = false;
    const uint64_t m_ticks;

    SpinLock m_overflowSpin;
    bool m_hasChildren = false;

    Data::Pool m_overflowPool;
    Data::PooledStack m_overflowStack;
    std::vector<Voxel> m_overflow;
    std::unique_ptr<std::vector<Key>> m_keys;


    /*
    Data::Pool m_overflowPool
    Data::PooledStack m_overflowStack;
    std::vector<ShallowVoxel> m_overflowShallowVoxels;
    */






    SpinLock m_spin;
    Data::Pool m_dataPool;
    Data::PooledStack m_stack;
    std::unique_ptr<std::vector<VoxelTube>> m_grid;

    std::vector<ReffedChunk> m_children;
};

} // namespace entwine

