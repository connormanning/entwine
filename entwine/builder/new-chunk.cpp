/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/new-chunk.hpp>

#include <entwine/builder/chunk-cache.hpp>
#include <entwine/io/io.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/voxel.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

NewChunk::NewChunk(const ChunkKey& ck, const Hierarchy& hierarchy)
    : m_metadata(ck.metadata())
    , m_span(m_metadata.span())
    , m_pointSize(m_metadata.schema().pointSize())
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
        if (!hierarchy.get(childAt(dir).dxyz()))
        {
            m_overflows[i] = makeUnique<Overflow>(ck.getStep(dir));
        }
    }
}

bool NewChunk::insert(ChunkCache& cache, Pruner& pruner, Voxel& voxel, Key& key)
{
    const Xyz& pos(key.position());
    const uint64_t i((pos.y % m_span) * m_span + (pos.x % m_span));
    auto& tube(m_grid[i]);

    UniqueSpin tubeLock(tube.spin);
    Voxel& dst(tube[pos.z]);

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

    return insertOverflow(cache, pruner, voxel, key);
}

bool NewChunk::insertOverflow(
        ChunkCache& cache,
        Pruner& pruner,
        Voxel& voxel,
        Key& key)
{
    if (m_chunkKey.depth() < m_metadata.overflowDepth()) return false;

    const Dir dir(getDirection(m_chunkKey.bounds().mid(), voxel.point()));
    const uint64_t i(toIntegral(dir));

    SpinGuard lock(m_overflowSpin);

    if (!m_overflows[i]) return false;
    if (!m_overflows[i]->insert(voxel, key)) return false;

    // Overflow inserted, update metric and perform overflow if needed.
    if (++m_overflowCount >= m_metadata.overflowThreshold())
    {
        maybeOverflow(cache, pruner);
    }

    return true;
}

void NewChunk::maybeOverflow(ChunkCache& cache, Pruner& pruner)
{
    // See if our resident size is big enough to overflow.
    uint64_t gridSize(0);
    {
        SpinGuard lock(m_spin);
        gridSize = m_gridBlock.size();
    }

    const uint64_t ourSize(gridSize + m_overflowCount);
    const uint64_t maxSize(
            m_span * m_span +   // TODO Make 3D.
            m_metadata.overflowThreshold());
    if (ourSize < maxSize) return;

    // Find the overflow with the largest point count.
    uint64_t selectedSize = 0;
    uint64_t selectedIndex = 0;
    for (uint64_t d(0); d < m_overflows.size(); ++d)
    {
        auto& current(m_overflows[d]);
        if (current && current->size() > selectedSize)
        {
            selectedIndex = d;
            selectedSize = current->size();
        }
    }

    // Make sure our largest overflow is large enough to necessitate a
    // child node.
    // TODO Make this ratio configurable.
    const uint64_t minSize(m_metadata.overflowThreshold() / 4.0);
    if (selectedSize < minSize) return;

    doOverflow(cache, pruner, selectedIndex);
}

void NewChunk::doOverflow(ChunkCache& cache, Pruner& pruner, uint64_t dir)
{
    assert(m_overflows[dir]);

    std::unique_ptr<Overflow> active;
    std::swap(m_overflows[dir], active);
    m_overflowCount -= active->size();

    // TODO We could unlock our overflowSpin here - bookkeeping has been
    // fully updated for the removal of this Overflow.

    const ChunkKey ck(m_childKeys[dir]);

    for (auto& entry : active->list())
    {
        entry.key.step(entry.voxel.point());
        cache.insert(entry.voxel, entry.key, ck, pruner);
    }
}

uint64_t NewChunk::save(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp) const
{
    uint64_t np(m_gridBlock.size());
    for (const auto& o : m_overflows) if (o) np += o->size();

    BlockPointTable table(m_metadata.schema());
    table.reserve(np);
    table.insert(m_gridBlock);
    for (auto& o : m_overflows) if (o) table.insert(o->block());

    const auto filename(
            m_chunkKey.toString() + m_metadata.postfix(m_chunkKey.depth()));
    m_metadata.dataIo().write(
            out,
            tmp,
            filename,
            m_chunkKey.bounds(),
            table);

    return np;
}

void NewChunk::load(
        ChunkCache& cache,
        Pruner& pruner,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        const uint64_t np)
{
    VectorPointTable table(m_metadata.schema(), np);
    table.setProcess([&]()
    {
        Voxel voxel;
        Key key(m_metadata);

        for (auto it(table.begin()); it != table.end(); ++it)
        {
            voxel.initShallow(it.pointRef(), it.data());
            key.init(voxel.point(), m_chunkKey.depth());
            cache.insert(voxel, key, m_chunkKey, pruner);
        }
    });

    const auto filename(
            m_chunkKey.toString() + m_metadata.postfix(m_chunkKey.depth()));
    m_metadata.dataIo().read(out, tmp, filename, table);
}

} // namespace entwine

