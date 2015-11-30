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
    std::size_t fetchesPerIteration(4);
}

Query::Query(
        const Reader& reader,
        const Schema& schema,
        Cache& cache,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool normalize)
    : m_reader(reader)
    , m_structure(reader.structure())
    , m_outSchema(schema)
    , m_cache(cache)
    , m_qbox(qbox)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_normalize(normalize)
    , m_table(reader.schema())
    , m_view(m_table)
    , m_chunks()
    , m_block()
    , m_chunkReaderIt()
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

            if (reader.exists(chunkId))
            {
                m_chunks.insert(
                        FetchInfo(
                            m_reader,
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

        if (m_chunks.empty())
        {
            m_done = true;
        }
        else if (buffer.empty())
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
    if (
            m_reader.base() &&
            m_depthBegin < m_structure.baseDepthEnd() &&
            m_depthEnd   > m_structure.baseDepthBegin())
    {
        const BaseChunk& base(*m_reader.base());
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
                processPoint(buffer, tube.primaryCell().atom().load()->val());

                for (const auto& c : tube.secondaryCells())
                {
                    processPoint(buffer, c.second.atom().load()->val());
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

void Query::processPoint(std::vector<char>& buffer, const PointInfo& info)
{
    if (m_qbox.contains(info.point()))
    {
        ++m_numPoints;
        buffer.resize(buffer.size() + m_outSchema.pointSize(), 0);
        char* pos(buffer.data() + buffer.size() - m_outSchema.pointSize());

        m_table.setData(info.data());
        bool isX(false), isY(false), isZ(false);

        for (const auto& dim : m_outSchema.dims())
        {
            if (m_normalize)
            {
                isX = dim.id() == pdal::Dimension::Id::X;
                isY = dim.id() == pdal::Dimension::Id::Y;
                isZ = dim.id() == pdal::Dimension::Id::Z;

                if (
                        (isX || isY || isZ) &&
                        pdal::Dimension::size(dim.type()) == 4)
                {
                    double d(m_view.getFieldAs<double>(dim.id(), 0));

                    if (isX)        d -= m_reader.bbox().mid().x;
                    else if (isY)   d -= m_reader.bbox().mid().y;
                    else            d -= m_reader.bbox().mid().z;

                    float f(d);

                    std::memcpy(pos, &f, 4);
                }
                else
                {
                    m_view.getField(pos, dim.id(), dim.type(), 0);
                }
            }
            else
            {
                m_view.getField(pos, dim.id(), dim.type(), 0);
            }

            pos += dim.size();
        }
    }
}

void Query::getChunked(std::vector<char>& buffer)
{
    if (!m_block)
    {
        if (m_chunks.size())
        {
            const auto begin(m_chunks.begin());
            auto end(m_chunks.begin());
            std::advance(end, std::min(fetchesPerIteration, m_chunks.size()));

            FetchInfoSet subset(begin, end);
            m_block = m_cache.acquire(m_reader.path(), subset);
            m_chunks.erase(begin, end);

            if (m_block) m_chunkReaderIt = m_block->chunkMap().begin();
        }
    }

    if (m_block)
    {
        if (const ChunkReader* cr = m_chunkReaderIt->second)
        {
            ChunkReader::QueryRange range(cr->candidates(m_qbox));
            auto it(range.begin);

            while (it != range.end)
            {
                processPoint(buffer, it->second);
                ++it;
            }

            if (++m_chunkReaderIt == m_block->chunkMap().end())
            {
                m_block.reset();
            }
        }
        else
        {
            throw std::runtime_error("Reservation failure");
        }
    }

    m_done = !m_block && m_chunks.empty();
}

} // namespace entwine

