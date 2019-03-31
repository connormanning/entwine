/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
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

#include <entwine/io/laszip.hpp>

namespace entwine
{

/*
struct NewVoxelTube
{
    SpinLock spin;
    std::map<uint64_t, Voxel> map;
};
*/

class NewChunk
{
    static constexpr uint64_t blockSize = 256;

    /*
    struct Overflow
    {
        Overflow(Key& key) : key(key) { }
        void step() { key.step(voxel.point()); }

        Key key;
        Voxel voxel;
    };
    */

public:
    /*
    using OverflowBlocks = std::array<MemBlock, 8>;
    using OverflowList = std::vector<Overflow>;
    using OverflowListPtr = std::unique_ptr<OverflowList>;
    */

    NewChunk(const Metadata& m)
        : m_metadata(m)
        , m_span(m.span())
        , m_pointSize(m.schema().pointSize())
        , m_grid(m_span * m_span * m_span)
        , m_gridData(m_grid.size() * m_pointSize)
        , m_gridSpins(m_grid.size())
          /*
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
        */
    {
        /*
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
        */
    }

    uint64_t save(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            const ChunkKey& ck)
    {
        // TODO Update IO methods so we don't have to transform our data.
        MemBlock block(m_pointSize, 1024);
        for (uint64_t i(0); i < m_grid.size(); ++i)
        {
            if (m_grid[i].data())
            {
                std::copy(
                        m_grid[i].data(),
                        m_grid[i].data() + m_pointSize,
                        block.next());
            }
        }

        const uint64_t np(block.size());

        BlockPointTable table(m_metadata.schema());
        table.insert(block);

        std::cout << "Writing " << ck.toString() << " " << np << std::endl;
        m_metadata.dataIo().write(
                out,
                tmp,
                ck.toString() + m_metadata.postfix(ck.depth()),
                ck.bounds(),
                table);

        return np;

    }

    bool insert(Voxel& voxel, Key& key)
    {
        const Xyz& pos(key.position());
        const uint64_t i(
                (pos.z % m_span) * m_span * m_span +
                (pos.y % m_span) * m_span +
                (pos.x % m_span));

        // TODO Assert.
        if (i > m_grid.size()) throw std::runtime_error("Invalid grid index");

        SpinGuard voxelSpin(m_gridSpins[i]);
        Voxel& dst(m_grid[i]);
        char* data(m_gridData.data() + i * m_pointSize);

        if (dst.data())
        {
            const Point& mid(key.bounds().mid());
            if (voxel.point().sqDist3d(mid) < dst.point().sqDist3d(mid))
            {
                voxel.swapDeep(dst, m_pointSize);
            }
        }
        else
        {
            dst.setData(data);
            dst.initDeep(voxel.point(), voxel.data(), m_pointSize);
            return true;
        }

        return false;
        // TODO - overflow here.
        /*
        voxelSpin.unlock();

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
        */
    }

    // MemBlock& gridBlock() { return m_gridBlock; }
    // OverflowBlocks& overflowBlocks() { return m_overflowBlocks; }

private:
    /*
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
                m_span * m_span * m_span +
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
    */

    const Metadata& m_metadata;
    const uint64_t m_span;
    const uint64_t m_pointSize;

    SpinLock m_spin;
    std::vector<Voxel> m_grid;
    std::vector<char> m_gridData;
    std::vector<SpinLock> m_gridSpins;

    /*
    SpinLock m_overflowSpin;
    OverflowBlocks m_overflowBlocks;
    std::array<OverflowListPtr, 8> m_overflowLists;
    uint64_t m_overflowCount = 0;
    */
};

} // namespace entwine

