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

namespace entwine
{

namespace
{
    std::mutex m;
    ReffedSelfChunk::Info info;
}

SelfChunk::SelfChunk(const ReffedSelfChunk& ref)
    : m_ref(ref)
{ }

void ReffedSelfChunk::ref(const NewClipper& clipper)
{
    const Origin o(clipper.origin());
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_refs.count(o))
    {
        m_refs[o] = 1;

        if (!m_chunk || m_chunk->acquired())
        {
            if (!m_chunk)
            {
                if (m_key.depth() < m_metadata.structure().tail())
                {
                    m_chunk = makeUnique<SelfContiguousChunk>(*this);
                }
                else
                {
                    m_chunk = makeUnique<SelfMappedChunk>(*this);
                }

                assert(!m_chunk->acquired());

                std::lock_guard<std::mutex> lock(m);
                ++info.count;
            }

            if (m_chunk->acquired())
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

                    if (!m_chunk->insert(pk, cell))
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

ReffedSelfChunk::Info ReffedSelfChunk::latchInfo()
{
    std::lock_guard<std::mutex> lock(m);

    Info result(info);
    info.clear();
    return result;
}

void ReffedSelfChunk::unref(const Origin o)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    assert(m_refs.count(o));

    if (!--m_refs.at(o))
    {
        m_refs.erase(o);
        if (m_refs.empty())
        {
            assert(m_chunk);

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

} // namespace entwine

