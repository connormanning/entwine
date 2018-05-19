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

/*
std::unique_ptr<SelfChunk> create(const ChunkKey& c)
{
    return makeUnique<SelfContiguousChunk>
}
*/

void ReffedSelfChunk::ref(const NewClimber& climber)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_refs.count(climber.origin()))
    {
        m_refs[climber.origin()] = 1;

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

            if (const uint64_t np = m_hierarchy.get(m_key.get()))
            {
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
}

void ReffedSelfChunk::unref(const Origin o)
{
    std::lock_guard<std::mutex> lock(m_mutex);

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

            m_chunk.reset();
        }
    }
}

} // namespace entwine

