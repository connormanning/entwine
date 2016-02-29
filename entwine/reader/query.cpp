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

BaseQuery::BaseQuery(
        const Reader& reader,
        Cache& cache,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : m_reader(reader)
    , m_structure(reader.structure())
    , m_cache(cache)
    , m_qbox(qbox)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
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
    }
}

bool BaseQuery::next()
{
    if (m_done) throw std::runtime_error("Called next after query completed");

    if (m_base)
    {
        m_base = false;

        if (!getBase())
        {
            if (m_chunks.empty()) m_done = true;
            else getChunked();
        }
    }
    else
    {
        getChunked();
    }

    return !m_done;
}

bool BaseQuery::getBase()
{
    bool dataExisted(false);

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
            return dataExisted;
        }

        do
        {
            terminate = false;

            const Id& index(splitter.index());
            const Tube& tube(base.getTube(index));

            if (!tube.empty())
            {
                if (processPoint(tube.primaryCell().atom().load()->val()))
                {
                    ++m_numPoints;
                    dataExisted = true;
                }

                for (const auto& c : tube.secondaryCells())
                {
                    if (processPoint(c.second.atom().load()->val()))
                    {
                        ++m_numPoints;
                        dataExisted = true;
                    }
                }
            }
            else
            {
                terminate = true;
            }
        }
        while (splitter.next(terminate));
    }

    return dataExisted;
}

void BaseQuery::getChunked()
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
                if (processPoint(it->second)) ++m_numPoints;
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

Query::Query(
        const Reader& reader,
        const Schema& schema,
        Cache& cache,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool normalize,
        const double scale)
    : BaseQuery(reader, cache, qbox, depthBegin, depthEnd)
    , m_buffer(nullptr)
    , m_outSchema(schema)
    , m_normalize(normalize || scale)
    , m_scale(scale)
    , m_readerMid(m_reader.bbox().mid())
    , m_table(reader.schema())
    , m_pointRef(m_table, 0)
{
    if (m_scale)
    {
        for (const auto& dim : m_outSchema.dims())
        {
            const bool isX = dim.id() == pdal::Dimension::Id::X;
            const bool isY = dim.id() == pdal::Dimension::Id::Y;
            const bool isZ = dim.id() == pdal::Dimension::Id::Z;

            if (isX || isY || isZ)
            {
                if (dim.size() < 4)
                {
                    throw std::runtime_error("Need at least 4 bytes to scale");
                }

                if (pdal::Dimension::base(dim.type()) ==
                        pdal::Dimension::BaseType::Unsigned)
                {
                    throw std::runtime_error("Scaled types must be signed");
                }
            }
        }
    }
}

bool Query::next(std::vector<char>& buffer)
{
    if (buffer.size()) throw std::runtime_error("Query buffer not empty");
    m_buffer = &buffer;

    return BaseQuery::next();
}

bool Query::processPoint(const PointInfo& info)
{
    if (!m_buffer) throw std::runtime_error("Query buffer not set");

    if (m_qbox.contains(info.point()))
    {
        m_buffer->resize(m_buffer->size() + m_outSchema.pointSize(), 0);
        char* pos(
                m_buffer->data() + m_buffer->size() - m_outSchema.pointSize());

        m_table.setPoint(info.data());
        bool isX(false), isY(false), isZ(false);

        bool written(false);

        for (const auto& dim : m_outSchema.dims())
        {
            written = false;

            if (m_normalize)
            {
                isX = dim.id() == pdal::Dimension::Id::X;
                isY = dim.id() == pdal::Dimension::Id::Y;
                isZ = dim.id() == pdal::Dimension::Id::Z;

                if (isX || isY || isZ)
                {
                    double d(m_pointRef.getFieldAs<double>(dim.id()));
                    const std::size_t dimSize(dim.size());

                    if (isX)        d -= m_readerMid.x;
                    else if (isY)   d -= m_readerMid.y;
                    else            d -= m_readerMid.z;

                    if (m_scale)
                    {
                        d /= m_scale;
                        written = true;

                        if (pdal::Dimension::base(dim.type()) ==
                                pdal::Dimension::BaseType::Floating)
                        {
                            if (dimSize == 4)
                            {
                                const float val(d);
                                std::memcpy(pos, &val, 4);
                            }
                            else
                            {
                                std::memcpy(pos, &d, 8);
                            }
                        }
                        else
                        {
                            // Type is signed, unsigned would have thrown in
                            // the constructor.  We also know that the size is
                            // 4 or 8.
                            if (dimSize == 4)
                            {
                                const int32_t val(d);
                                std::memcpy(pos, &val, 4);
                            }
                            else
                            {
                                const uint64_t val(d);
                                std::memcpy(pos, &val, 8);
                            }
                        }
                    }
                    else if (
                            dimSize == 4 &&
                            pdal::Dimension::base(dim.type()) ==
                                pdal::Dimension::BaseType::Floating)
                    {
                        written = true;
                        const float f(d);
                        std::memcpy(pos, &f, 4);
                    }
                }
            }

            if (!written)
            {
                m_pointRef.getField(pos, dim.id(), dim.type());
            }

            pos += dim.size();
        }

        return true;
    }
    else
    {
        return false;
    }
}

} // namespace entwine

