/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/chunk.hpp>

#include <entwine/builder/chunk-cache.hpp>
#include <entwine/io/io.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/voxel.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Chunk::Chunk(const Metadata& m, const ChunkKey& ck, const Hierarchy& hierarchy)
    : m_metadata(m)
    , m_span(m_metadata.span)
    , m_pointSize(getPointSize(m_metadata.absoluteSchema))
    , m_chunkKey(ck)
    , m_childKeys { {
        ck.getStep(toDir(0)),
        ck.getStep(toDir(1)),
        ck.getStep(toDir(2)),
        ck.getStep(toDir(3)),
        ck.getStep(toDir(4)),
        ck.getStep(toDir(5)),
        ck.getStep(toDir(6)),
        ck.getStep(toDir(7))
    } }
    , m_grid(m_span * m_span)
    , m_gridBlock(m_pointSize, 4096)
{
    for (uint64_t i(0); i < dirEnd(); ++i)
    {
        const Dir dir(toDir(i));

        // If there are already points here, it gets no overflow.
        if (!hierarchy::get(hierarchy, childAt(dir).dxyz()))
        {
            m_overflows[i] = makeUnique<Overflow>(ck.getStep(dir), m_pointSize);
        }
    }
}

bool Chunk::insert(ChunkCache& cache, Clipper& clipper, Voxel& voxel, Key& key)
{
    const Xyz& pos(key.position());
    const uint64_t i((pos.y % m_span) * m_span + (pos.x % m_span));
    auto& tube(m_grid[i]);

    UniqueSpin tubeLock(tube.spin);
    Voxel& dst(tube.map[pos.z]);

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
        {
            SpinGuard lock(m_spin);
            dst.setData(m_gridBlock.next());
        }
        dst.initDeep(voxel.point(), voxel.data(), m_pointSize);
        return true;
    }

    tubeLock.unlock();

    return insertOverflow(cache, clipper, voxel, key);
}

bool Chunk::insertOverflow(
        ChunkCache& cache,
        Clipper& clipper,
        Voxel& voxel,
        Key& key)
{
    if (m_chunkKey.depth() < getSharedDepth(m_metadata)) return false;

    const Dir dir(getDirection(m_chunkKey.bounds().mid(), voxel.point()));
    const uint64_t i(toIntegral(dir));

    SpinGuard lock(m_overflowSpin);

    if (!m_overflows[i]) return false;

    m_overflows[i]->insert(voxel, key);

    // Overflow inserted, update metric and perform overflow if needed.
    if (++m_overflowCount >= m_metadata.internal.minNodeSize)
    {
        maybeOverflow(cache, clipper);
    }

    return true;
}

void Chunk::maybeOverflow(ChunkCache& cache, Clipper& clipper)
{
    // See if our resident size is big enough to overflow.
    uint64_t gridSize(0);
    {
        SpinGuard lock(m_spin);
        gridSize = m_gridBlock.size();
    }

    const uint64_t ourSize(gridSize + m_overflowCount);
    if (ourSize < m_metadata.internal.maxNodeSize) return;

    // Find the overflow with the largest point count.
    uint64_t selectedSize = 0;
    uint64_t selectedIndex = 0;
    for (uint64_t d(0); d < m_overflows.size(); ++d)
    {
        auto& current(m_overflows[d]);
        if (current && current->block.size() > selectedSize)
        {
            selectedIndex = d;
            selectedSize = current->block.size();
        }
    }

    // Make sure our largest overflow is large enough to necessitate
    // overflowing into its own node.
    if (selectedSize < m_metadata.internal.minNodeSize) return;

    doOverflow(cache, clipper, selectedIndex);
}

void Chunk::doOverflow(ChunkCache& cache, Clipper& clipper, uint64_t dir)
{
    assert(m_overflows[dir]);

    std::unique_ptr<Overflow> active;
    std::swap(m_overflows[dir], active);
    m_overflowCount -= active->block.size();

    // TODO We could unlock our overflowSpin here - bookkeeping has been
    // fully updated for the removal of this Overflow.

    const ChunkKey ck(m_childKeys[dir]);

    for (auto& entry : active->list)
    {
        entry.key.step(entry.voxel.point());
        cache.insert(entry.voxel, entry.key, ck, clipper);
    }
}

uint64_t Chunk::save(const Endpoints& endpoints) const
{
    uint64_t np(m_gridBlock.size());
    for (const auto& o : m_overflows) if (o) np += o->block.size();

    auto layout = toLayout(m_metadata.absoluteSchema);
    BlockPointTable table(layout);
    table.reserve(np);
    table.insert(m_gridBlock);
    for (auto& o : m_overflows) if (o) table.insert(o->block);

    const auto filename =
        m_chunkKey.toString() + getPostfix(m_metadata, m_chunkKey.depth());

    io::write(
        m_metadata.dataType,
        m_metadata,
        endpoints,
        filename,
        table,
        m_chunkKey.bounds());

    return np;
}

void Chunk::load(
        ChunkCache& cache,
        Clipper& clipper,
        const Endpoints& endpoints,
        const uint64_t np)
{
    auto layout = toLayout(m_metadata.absoluteSchema);
    VectorPointTable table(layout, np);
    table.setProcess([&]()
    {
        Voxel voxel;
        Key key(m_metadata.bounds, getStartDepth(m_metadata));

        for (auto it = table.begin(); it != table.end(); ++it)
        {
            voxel.initShallow(it.pointRef(), it.data());
            key.init(voxel.point(), m_chunkKey.depth());
            cache.insert(voxel, key, m_chunkKey, clipper);
        }
    });

    const auto filename =
        m_chunkKey.toString() + getPostfix(m_metadata, m_chunkKey.depth());

    io::read(m_metadata.dataType, m_metadata, endpoints, filename, table);
}

} // namespace entwine
