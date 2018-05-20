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
    std::size_t n(0);
    ReffedSelfChunk::Info info;
}

void ReffedSelfChunk::ref(const NewClimber& climber)
{
    const Origin o(climber.origin());
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_refs.count(o))
    {
        m_refs[o] = 1;

        if (!m_chunk || m_chunk->written())
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

                std::lock_guard<std::mutex> lock(m);
                ++n;
            }

            if (m_chunk->written())
            {
                m_chunk->init();

                std::lock_guard<std::mutex> lock(m);
                ++n;
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

                NewClimber c(climber);

                while (!cells.empty())
                {
                    auto cell(cells.popOne());
                    c.init(cell->point(), m_key.depth());
                    if (!m_chunk->insert(cell, c, nullptr))
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

std::size_t ReffedSelfChunk::count()
{
    std::lock_guard<std::mutex> lock(m);
    return n;
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
            auto cells(m_chunk->acquire(m_pointPool));

            uint64_t np(0);
            for (const Cell& cell : cells) np += cell.size();
            m_hierarchy.set(m_key.get(), np);

            m_metadata.storage().write(
                    m_out,
                    m_tmp,
                    m_pointPool,
                    m_key.toString() + m_metadata.postfix(m_key.depth()),
                    std::move(cells));

            std::lock_guard<std::mutex> lock(m);
            --n;
            ++info.written;
        }
    }
}

} // namespace entwine

