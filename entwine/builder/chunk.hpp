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
#include <utility>

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/types/voxel.hpp>
#include <entwine/util/spin-lock.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Chunk;

class ReffedChunk
{
public:
    ReffedChunk(
            const ChunkKey& key,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            Hierarchy& hierarchy);

    ReffedChunk(const ReffedChunk& o);
    ~ReffedChunk();

    struct Info
    {
        std::size_t written = 0;
        std::size_t read = 0;
        std::size_t alive = 0;
    };

    bool insert(Voxel& voxel, Key& key, Clipper& clipper);

    void ref(Clipper& clipper);
    void unref(Origin o);
    bool empty();

    Chunk& chunk() { assert(m_chunk); return *m_chunk; }

    const ChunkKey& key() const { return m_key; }
    const Metadata& metadata() const { return m_metadata; }
    const arbiter::Endpoint& out() const { return m_out; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }
    Hierarchy& hierarchy() const { return m_hierarchy; }

    static Info latchInfo();

private:
    ChunkKey m_key;
    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    Hierarchy& m_hierarchy;

    SpinLock m_spin;
    std::unique_ptr<Chunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

struct VoxelTube
{
    SpinLock spin;
    std::map<uint64_t, Voxel> map;
};

class Chunk
{
    static constexpr uint64_t blockSize = 256;

    struct Overflow
    {
        Overflow(Key& key) : key(key) { }
        void step() { key.step(voxel.point()); }

        Key key;
        Voxel voxel;
    };

public:
    using OverflowBlocks = std::array<MemBlock, 8>;
    using OverflowList = std::vector<Overflow>;
    using OverflowListPtr = std::unique_ptr<OverflowList>;

    Chunk(const ReffedChunk& ref)
        : m_ref(ref)
        , m_span(m_ref.metadata().span())
        , m_pointSize(m_ref.metadata().schema().pointSize())
        , m_gridBlock(m_pointSize, 4096)
        , m_overflowBlocks { {
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize),
            MemBlock(m_pointSize, blockSize)
        } }
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
                    m_ref.hierarchy());
        }
    }

    void init()
    {
        assert(!m_grid);
        m_grid = makeUnique<std::vector<VoxelTube>>(m_span * m_span);

        for (std::size_t d(0); d < dirEnd(); ++d)
        {
            assert(!m_overflowLists[d]);
            const ChunkKey key(m_ref.key().getStep(toDir(d)));
            if (!m_ref.hierarchy().get(key.get()))
            {
                m_overflowLists[d] = makeUnique<OverflowList>();
            }
        }

        m_remote = false;
    }

    bool terminus()
    {
        // Make sure we don't early-return here - need to traverse all children.
        bool result(true);
        for (auto& c : m_children) if (!c.empty()) result = false;
        return result;
    }

    void reset()
    {
        m_grid.reset();
        m_overflowCount = 0;
        for (std::size_t d(0); d < dirEnd(); ++d) m_overflowLists[d].reset();
        m_remote = true;

        m_gridBlock.clear();
        for (auto& o : m_overflowBlocks) o.clear();
    }

    bool remote() const { return m_remote; }

    ReffedChunk& step(const Point& p)
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return m_children[toIntegral(dir)];
    }

    bool insert(Voxel& voxel, Key& key, Clipper& clipper)
    {
        const Xyz& pos(key.position());
        const std::size_t i((pos.y % m_span) * m_span + (pos.x % m_span));
        VoxelTube& tube((*m_grid)[i]);

        UniqueSpin tubeLock(tube.spin);
        Voxel& dst(tube.map[pos.z]);

        if (dst.data())
        {
            const Point& mid(key.bounds().mid());
            if (voxel.point().sqDist3d(mid) < dst.point().sqDist3d(mid))
            {
                if (!insertOverflow(dst, key, clipper))
                {
                    key.step(dst.point());
                    step(dst.point()).insert(dst, key, clipper);
                }

                dst.initDeep(voxel.point(), voxel.data(), m_pointSize);
                return true;
            }
        }
        else
        {
            {
                SpinGuard lock(m_spin);
                dst.setData(m_gridBlock.next());
            }
            dst.initDeep(voxel.point(), voxel.data(), m_pointSize);
            return true;
        }

        tubeLock.unlock();

        if (insertOverflow(voxel, key, clipper))
        {
            return true;
        }
        else
        {
            key.step(voxel.point());
            step(voxel.point()).insert(voxel, key, clipper);
            return false;
        }
    }

    MemBlock& gridBlock() { return m_gridBlock; }
    OverflowBlocks& overflowBlocks() { return m_overflowBlocks; }

private:
    bool insertOverflow(Voxel& voxel, Key& key, Clipper& clipper)
    {
        if (m_ref.key().depth() < m_ref.metadata().overflowDepth())
        {
            return false;
        }

        const Dir d(getDirection(m_ref.key().bounds().mid(), voxel.point()));
        const uint64_t i(toIntegral(d));

        OverflowListPtr& o(m_overflowLists[i]);
        MemBlock& b(m_overflowBlocks[i]);

        SpinGuard lock(m_overflowSpin);
        if (!o) return false;
        insertOverflow(voxel, key, clipper, o, b);
        return true;
    }

    void insertOverflow(
            Voxel& voxel,
            Key& key,
            Clipper& clipper,
            OverflowListPtr& o,
            MemBlock& b)
    {
        // Insert the voxel into the proper overflow.
        assert(o);
        Overflow overflow(key);
        overflow.voxel.setData(b.next());
        overflow.voxel.initDeep(voxel.point(), voxel.data(), m_pointSize);
        o->push_back(overflow);

        if (++m_overflowCount < m_ref.metadata().overflowThreshold()) return;

        // See if our resident size is big enough to overflow.
        uint64_t gridSize(0);
        {
            SpinGuard lock(m_spin);
            gridSize = m_gridBlock.size();
        }

        const uint64_t ourSize(gridSize + m_overflowCount);
        const uint64_t maxSize(
                m_span * m_span +
                m_ref.metadata().overflowThreshold());
        if (ourSize < maxSize) return;

        // Find the overflow with the largest point count.
        uint64_t selectedSize = 0;
        uint64_t selectedIndex = 0;
        for (uint64_t d(0); d < m_overflowLists.size(); ++d)
        {
            auto& current(m_overflowLists[d]);
            if (current && current->size() > selectedSize)
            {
                selectedIndex = d;
                selectedSize = current->size();
            }
        }

        // Make sure our largest overflow is large enough to necessitate a
        // child node.
        const uint64_t minSize(m_ref.metadata().overflowThreshold() / 2.0);
        if (selectedSize < minSize) return;

        doOverflow(clipper, selectedIndex);
    }

    void doOverflow(Clipper& clipper, const uint64_t index)
    {
        OverflowListPtr& olist(m_overflowLists[index]);
        MemBlock& b(m_overflowBlocks[index]);
        assert(olist);

        for (Overflow& o : *olist)
        {
            o.step();
            m_children[index].insert(o.voxel, o.key, clipper);
        }

        m_overflowCount -= olist->size();
        olist.reset();
        b.clear();
    }

    const ReffedChunk& m_ref;
    const uint64_t m_span;
    const uint64_t m_pointSize;
    bool m_remote = false;

    SpinLock m_spin;
    std::unique_ptr<std::vector<VoxelTube>> m_grid;
    MemBlock m_gridBlock;

    SpinLock m_overflowSpin;
    OverflowBlocks m_overflowBlocks;
    std::array<OverflowListPtr, 8> m_overflowLists;
    uint64_t m_overflowCount = 0;

    std::vector<ReffedChunk> m_children;
};

} // namespace entwine

