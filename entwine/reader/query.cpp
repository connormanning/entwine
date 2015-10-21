/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/query.hpp>

#include <iterator>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/tree/cell.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

namespace
{
    std::size_t chunksPerIteration(4);
}

Query::Query(
        const Reader& reader,
        const Structure& structure,
        const Schema& schema,
        Cache& cache,
        const BBox& qbox,
        std::size_t depthBegin,
        std::size_t depthEnd)
    : m_reader(reader)
    , m_structure(structure)
    , m_outSchema(schema)
    , m_cache(cache)
    , m_qbox(qbox)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_chunks()
    , m_numPoints(0)
    , m_base(true)
    , m_done(false)
{
    if (!m_depthEnd || m_depthEnd > m_structure.coldDepthBegin())
    {
        SplitClimber splitter(
                m_structure,
                m_reader.bbox(),
                m_qbox,
                m_depthBegin,
                m_depthEnd,
                true);

        bool terminate(false);

        do
        {
            terminate = false;
            const Id& chunkId(splitter.index());

            if (arbiter::Endpoint* endpoint = reader.getEndpoint(chunkId))
            {
                m_chunks.insert(
                        FetchInfo(
                            *endpoint,
                            m_reader.schema(),
                            m_reader.bbox(),
                            chunkId,
                            m_structure.getInfo(chunkId).chunkPoints(),
                            splitter.depth()));
            }
            else
            {
                terminate = true;
            }
        }
        while (splitter.next(terminate));

        std::cout << "Chunks in query: " << m_chunks.size() << std::endl;
    }
}


void Query::next(std::vector<char>& buffer)
{
    if (!buffer.empty()) throw std::runtime_error("Buffer should be empty");
    if (m_done) throw std::runtime_error("Called next after query completed");

    if (m_base)
    {
        getBase(buffer);
        m_base = false;

        if (buffer.empty())
        {
            getChunked(buffer);
        }
    }
    else
    {
        getChunked(buffer);
    }
}

void Query::getBase(std::vector<char>& buffer)
{
    std::cout << "Base..." << std::endl;
    SinglePointTable table(m_reader.schema());
    LinkingPointView view(table);

    if (
            m_reader.base() &&
            m_depthBegin < m_structure.baseDepthEnd() &&
            m_depthEnd   > m_structure.baseDepthBegin())
    {
        const ContiguousChunk& base(*m_reader.base());
        bool terminate(false);
        SplitClimber splitter(
                m_structure,
                m_reader.bbox(),
                m_qbox,
                m_depthBegin,
                std::min(m_depthEnd, m_structure.baseDepthEnd()));

        if (splitter.index() < m_structure.baseIndexBegin())
        {
            std::cout << "Nothing selected in base" << std::endl;
            return;
        }

        do
        {
            terminate = false;

            const Id& index(splitter.index());
            const Tube& tube(base.getTube(index));

            if (!tube.empty())
            {
                auto processCell([&](const Cell& cell)
                {
                    const auto& info(cell.atom().load()->val());

                    if (m_qbox.contains(info.point()))
                    {
                        ++m_numPoints;

                        std::size_t initialSize(buffer.size());
                        buffer.resize(initialSize + m_outSchema.pointSize());
                        char* out(buffer.data() + initialSize);

                        table.setData(info.data());

                        for (const auto& dim : m_outSchema.dims())
                        {
                            view.getField(out, dim.id(), dim.type(), 0);
                            out += dim.size();
                        }
                    }
                });

                processCell(tube.primaryCell());

                for (const auto& c : tube.secondaryCells())
                {
                    processCell(c.second);
                }
            }
            else
            {
                terminate = true;
            }
        }
        while (splitter.next(terminate));
    }
    std::cout << "Base done" << std::endl;
}

void Query::getChunked(std::vector<char>& buffer)
{
    const auto begin(m_chunks.begin());
    auto end(m_chunks.begin());
    std::advance(end, std::min(chunksPerIteration, m_chunks.size()));

    FetchInfoSet subset(begin, end);
    std::unique_ptr<Block> block(m_cache.acquire(m_reader.path(), subset));
    m_chunks.erase(begin, end);

    for (const auto& p : block->chunkMap())
    {
        if (const ChunkReader* cr = p.second)
        {
            m_numPoints += cr->query(buffer, m_outSchema, m_qbox);
        }
        else
        {
            throw std::runtime_error("Reservation failure");
        }
    }

    if (buffer.empty()) m_done = true;
}

} // namespace entwine

