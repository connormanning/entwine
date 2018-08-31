/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/chunk.hpp>

#include <entwine/io/io.hpp>
#include <entwine/io/laszip.hpp>

namespace entwine
{

namespace
{
    SpinLock spin;
    ReffedChunk::Info info;
}

ReffedChunk::ReffedChunk(
        const ChunkKey& key,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        Hierarchy& hierarchy)
    : m_key(key)
    , m_metadata(m_key.metadata())
    , m_out(out)
    , m_tmp(tmp)
    , m_pointPool(pointPool)
    , m_hierarchy(hierarchy)
{ }

ReffedChunk::ReffedChunk(const ReffedChunk& o)
    : ReffedChunk(
            o.key(),
            o.out(),
            o.tmp(),
            o.pointPool(),
            o.hierarchy())
{
    // This happens only during the constructor of the chunk.
    assert(!o.m_chunk);
    assert(o.m_refs.empty());
}

ReffedChunk::~ReffedChunk() { }

void ReffedChunk::insert(Voxel& voxel, Key& key, Clipper& clipper)
{
    if (clipper.insert(*this)) ref(clipper);
    m_chunk->insert(voxel, key, clipper);
}

void ReffedChunk::ref(Clipper& clipper)
{
    const Origin o(clipper.origin());
    SpinGuard lock(m_spin);

    if (!m_refs.count(o))
    {
        m_refs[o] = 1;

        if (!m_chunk || m_chunk->remote())
        {
            if (!m_chunk)
            {
                m_chunk = makeUnique<Chunk>(*this);
                assert(!m_chunk->remote());
            }

            if (m_chunk->remote())
            {
                m_chunk->init();
            }

            if (const uint64_t np = m_hierarchy.get(m_key.get()))
            {
                {
                    SpinGuard lock(spin);
                    ++info.read;
                }

                // TODO We should read directly into our Chunk's DataPool to
                // avoid an initial copy.
                Cells cells = m_metadata.dataIo().read(
                        m_out,
                        m_tmp,
                        m_pointPool,
                        m_key.toString() + m_metadata.postfix(m_key.depth()));

                assert(cells.size() == np);

                Key pk(m_metadata);

                Voxel voxel;
                Data::RawNode node;
                voxel.stack().push(&node);

                while (!cells.empty())
                {
                    auto cell(cells.popOne());
                    pk.init(cell->point(), m_key.depth());

                    *node = cell->uniqueData();
                    voxel.point() = cell->point();

                    insert(voxel, pk, clipper);
                }
            }
        }
    }
    else ++m_refs[o];
}

void ReffedChunk::unref(const Origin o)
{
    SpinGuard lock(m_spin);

    assert(m_chunk);
    assert(m_refs.count(o));

    if (!--m_refs.at(o))
    {
        m_refs.erase(o);
        if (m_refs.empty())
        {
            Data::RawStack data(m_chunk->acquireBinary());
            const uint64_t np(data.size());

            m_hierarchy.set(m_key.get(), np);

            reinterpret_cast<const Laz&>(m_metadata.dataIo()).write(
                    m_out,
                    m_tmp,
                    m_metadata,
                    m_key.toString() + m_metadata.postfix(m_key.depth()),
                    m_key.bounds(),
                    data);

            SpinGuard lock(spin);
            ++info.written;
        }
    }
}

bool ReffedChunk::empty()
{
    SpinGuard lock(m_spin);

    if (!m_chunk) return true;

    if (m_chunk->terminus() && m_refs.empty())
    {
        m_chunk.reset();
        return true;
    }

    return false;
}

ReffedChunk::Info ReffedChunk::latchInfo()
{
    SpinGuard lock(spin);

    Info result(info);
    info.clear();
    return result;
}

} // namespace entwine

