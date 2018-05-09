/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/split-chunk.hpp>

#include <entwine/tree/slice.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<SplitChunk> SplitChunk::create(
        bool contiguous,
        std::size_t splits)
{
    if (contiguous) return makeUnique<ContiguousSplitChunk>(splits);
    else return makeUnique<MappedSplitChunk>();
}

void ReffedChunk::ref(const Slice& s, const NewClimber& climber)
{
    const Origin o(climber.origin());
    const Xyz& ck(climber.chunkKey().position());

    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_refs.count(o))
    {
        m_refs[o] = 1;

        if (!m_chunk)
        {
            m_chunk = s.create();
            if (m_np)
            {
                Cells cells(s.read(ck));
                assert(cells.size() == m_np);

                NewClimber c(climber);

                while (!cells.empty())
                {
                    auto cell(cells.popOne());
                    c.init(cell->point(), s.depth());
                    if (!m_chunk->insert(cell, c).done())
                    {
                        throw std::runtime_error(
                                "Invalid wakeup: " +
                                climber.chunkKey().position()
                                    .toString(s.depth()));
                    }
                }
            }
        }
    }
    else ++m_refs[o];
}

void ReffedChunk::unref(const Slice& s, const Xyz& p, uint64_t o)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!--m_refs.at(o))
    {
        m_refs.erase(o);
        if (m_refs.empty())
        {
            auto cells(m_chunk->acquire(s.pointPool()));

            m_np = 0;
            for (const Cell& cell : cells) m_np += cell.size();
            s.write(p, std::move(cells));

            m_chunk.reset();
        }
    }
}

} // namespace entwine

