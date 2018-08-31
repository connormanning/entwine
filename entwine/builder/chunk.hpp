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
        , m_dataPool(m_ref.metadata().schema().pointSize(), 4096)
        , m_dataStack(m_dataPool)
        , m_overflowPool(m_ref.metadata().schema().pointSize(), 1024)
        , m_overflowStack(m_overflowPool)
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
        assert(!m_grid);
        m_grid = makeUnique<std::vector<VoxelTube>>(m_ticks * m_ticks);
        assert(!m_overflow);
        m_overflow = makeUnique<std::vector<Overflow>>();
        m_remote = false;
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
        m_overflow.reset();

        m_remote = true;

        Data::RawStack stack(m_dataStack.get());
        stack.push(m_overflowStack.get());

        return stack;
    }

    bool remote() const { return m_remote; }

    ReffedChunk& step(const Point& p)
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return m_children[toIntegral(dir)];
    }

    void insert(Voxel& voxel, Key& key, Clipper& clipper)
    {
        if (!insertNative(voxel, key) && !insertOverflow(voxel, key, clipper))
        {
            key.step(voxel.point());
            step(voxel.point()).insert(voxel, key, clipper);
        }
    }

private:
    bool insertNative(Voxel& voxel, Key& key)
    {
        const Xyz& pos(key.position());
        const std::size_t i((pos.y % m_ticks) * m_ticks + (pos.x % m_ticks));
        VoxelTube& tube((*m_grid)[i]);

        UniqueSpin tubeLock(tube.spin);
        Voxel& dst(tube.map[pos.z]);

        if (dst.accepts(voxel))
        {
            m_dataStack.push(dst.push(voxel, m_dataPool));
            return true;
        }
        return false;
    }

    bool insertOverflow(Voxel& voxel, Key& key, Clipper& clipper)
    {
        if (m_ref.key().depth() < m_ref.metadata().overflowDepth())
        {
            return false;
        }

        SpinGuard lock(m_overflowSpin);
        if (m_hasChildren) return false;

        assert(m_overflow);

        Overflow overflow(key);
        m_overflowStack.push(overflow.voxel.push(voxel, m_overflowPool));
        m_overflow->push_back(overflow);

        if (m_overflowStack.size() > m_ref.metadata().overflowThreshold())
        {
            doOverflow(clipper);
        }

        return true;
    }

    void doOverflow(Clipper& clipper)
    {
        m_hasChildren = true;

        for (std::size_t i(0); i < m_overflow->size(); ++i)
        {
            Overflow& o((*m_overflow)[i]);
            o.step();
            step(o.voxel.point()).insert(o.voxel, o.key, clipper);
        }

        m_overflow.reset();
        m_overflowStack.reset();
    }

    const ReffedChunk& m_ref;
    bool m_remote = false;
    const uint64_t m_ticks;

    SpinLock m_spin;
    Data::Pool m_dataPool;
    Data::PooledStack m_dataStack;
    std::unique_ptr<std::vector<VoxelTube>> m_grid;

    SpinLock m_overflowSpin;
    bool m_hasChildren = false;
    Data::Pool m_overflowPool;
    Data::PooledStack m_overflowStack;

    struct Overflow
    {
        Overflow(Key& key) : key(key) { }
        void step() { key.step(voxel.point()); }

        Key key;
        Voxel voxel;
    };
    std::unique_ptr<std::vector<Overflow>> m_overflow;

    std::vector<ReffedChunk> m_children;
};

} // namespace entwine

