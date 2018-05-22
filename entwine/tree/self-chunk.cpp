/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/self-chunk.hpp>

#include <entwine/types/chunk-storage/chunk-storage.hpp>

namespace entwine
{

namespace
{
    std::mutex m;
    ReffedFixedChunk::Info info;
}

ReffedFixedChunk::ReffedFixedChunk(
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
{
    std::lock_guard<std::mutex> lock(m);
    ++info.reffed;
}

ReffedFixedChunk::ReffedFixedChunk(const ReffedFixedChunk& o)
    : ReffedFixedChunk(
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

ReffedFixedChunk::~ReffedFixedChunk()
{
    std::lock_guard<std::mutex> lock(m);
    --info.reffed;
}

bool ReffedFixedChunk::insert(
        Cell::PooledNode& cell,
        const Key& key,
        NewClipper& clipper)
{
    if (clipper.insert(*this)) ref(clipper);
    return m_chunk->insert(key, cell, clipper);
}

void ReffedFixedChunk::ref(NewClipper& clipper)
{
    const Origin o(clipper.origin());
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_refs.count(o))
    {
        m_refs[o] = 1;

        if (!m_chunk || m_chunk->remote())
        {
            if (!m_chunk)
            {
                m_chunk = makeUnique<FixedChunk>(*this);
                assert(!m_chunk->remote());

                std::lock_guard<std::mutex> lock(m);
                ++info.count;
            }

            if (m_chunk->remote())
            {
                m_chunk->init();

                std::lock_guard<std::mutex> lock(m);
                ++info.count;
            }

            if (const uint64_t np = m_hierarchy.get(m_key.get()))
            {
                {
                    std::lock_guard<std::mutex> lock(m);
                    ++info.read;
                }

                Cells cells = m_metadata.storage().read(
                        m_out,
                        m_tmp,
                        m_pointPool,
                        m_key.toString() + m_metadata.postfix(m_key.depth()));

                assert(cells.size() == np);

                Key pk(m_metadata);

                while (!cells.empty())
                {
                    auto cell(cells.popOne());
                    pk.init(cell->point(), m_key.depth());

                    if (!insert(cell, pk, clipper))
                    {
                        throw std::runtime_error(
                                "Invalid wakeup: " + m_key.toString());
                    }
                }
            }
        }
    }
    else ++m_refs[o];
}

void ReffedFixedChunk::unref(const Origin o)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    assert(m_chunk);
    assert(m_refs.count(o));

    if (!--m_refs.at(o))
    {
        m_refs.erase(o);
        if (m_refs.empty())
        {
            CountedCells cells(m_chunk->acquire());

            m_hierarchy.set(m_key.get(), cells.np);

            m_metadata.storage().write(
                    m_out,
                    m_tmp,
                    m_pointPool,
                    m_key.toString() + m_metadata.postfix(m_key.depth()),
                    std::move(cells.stack));

            std::lock_guard<std::mutex> lock(m);
            --info.count;
            ++info.written;
        }
    }
}

bool ReffedFixedChunk::empty()
{
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_chunk) return true;

    if (m_chunk->terminus() && m_refs.empty())
    {
        m_chunk.reset();
        return true;
    }

    return false;
}

ReffedFixedChunk::Info ReffedFixedChunk::latchInfo()
{
    std::lock_guard<std::mutex> lock(m);

    Info result(info);
    info.clear();
    return result;
}









bool FixedChunk::insert(
        const Key& key,
        Cell::PooledNode& cell,
        NewClipper& clipper)
{
    if (insertNative(key, cell)) return true;
    if (m_ref.key().depth() < m_ref.metadata().overflowDepth()) return false;

    std::lock_guard<std::mutex> lock(m_overflowMutex);
    if (m_hasChildren) return false;

    m_overflow.push(std::move(cell));
    m_keys.push(key);

    assert(m_overflow.size() == m_keys.size());

    if (m_overflow.size() <= m_ref.metadata().overflowLimit()) return true;

    m_hasChildren = true;

    while (!m_overflow.empty())
    {
        auto curCell(m_overflow.popOne());
        Key curKey(m_keys.top());
        m_keys.pop();

        curKey.step(curCell->point());
        if (!step(curCell->point()).insert(curCell, curKey, clipper))
        {
            throw std::runtime_error("Invalid overflow");
        }

        assert(m_overflow.size() == m_keys.size());
    }

    assert(m_overflow.empty());
    assert(m_keys.empty());

    return true;
}

} // namespace entwine

