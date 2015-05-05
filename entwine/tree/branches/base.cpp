/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/base.hpp>

#include <cstring>
#include <limits>

#include <pdal/PointView.hpp>
#include <pdal/PointTable.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/http/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/branches/chunk.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    std::vector<char> makeEmptyPoint(const entwine::Schema& schema)
    {
        entwine::SimplePointTable table(schema);
        pdal::PointView view(table);

        view.setField(pdal::Dimension::Id::X, 0, Point::emptyCoord());
        view.setField(pdal::Dimension::Id::Y, 0, Point::emptyCoord());

        return table.data();
    }
}

BaseBranch::BaseBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthEnd)
    : Branch(source, schema, dimensions, depthBegin(), depthEnd)
    , m_chunk(new Chunk(schema, depthBegin(), indexSpan()))
{
    // TODO
    if (depthBegin()) throw std::runtime_error("Base starts at zero");
    m_ids.insert(indexBegin());
}

BaseBranch::BaseBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(source, schema, dimensions, meta)
    , m_chunk()
{
    // TODO Allow 'null' branch above this one.
    if (depthBegin()) throw std::runtime_error("Base starts at zero");

    load(meta);
}

BaseBranch::~BaseBranch()
{ }

std::unique_ptr<Entry> BaseBranch::getEntry(const std::size_t index)
{
    return m_chunk->getEntry(index);
}

void BaseBranch::saveImpl(Json::Value& meta)
{
    const uint64_t uncSize(m_chunk->data().size());

    std::unique_ptr<std::vector<char>> compressed(
            Compression::compress(m_chunk->data(), schema()));

    Compression::pushSize(*compressed, uncSize);

    m_source.put(std::to_string(indexBegin()), *compressed);
}

void BaseBranch::load(const Json::Value& meta)
{
    if (m_ids.size() != 1)
    {
        throw std::runtime_error("Invalid serialized base branch.");
    }

    std::vector<char> compressed(m_source.get(std::to_string(indexBegin())));
    const std::size_t uncSize(Compression::popSize(compressed));

    std::unique_ptr<std::vector<char>> uncompressed(
            Compression::decompress(compressed, schema(), uncSize));

    m_chunk.reset(new Chunk(schema(), indexBegin(), *uncompressed));
}

void BaseBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    const std::size_t pointSize(schema().pointSize());
    const std::vector<char> emptyPoint(makeEmptyPoint(schema()));

    {
        auto compressed(
                Compression::compress(
                    m_chunk->data().data(),
                    start * schema().pointSize(),
                    schema()));

        ids.push_back(0);
        output.put(std::to_string(0), *compressed);
    }

    for (std::size_t id(start); id < indexEnd(); id += chunkPoints)
    {
        ids.push_back(id);

        const char* pos(m_chunk->data().data() + id * pointSize);
        std::vector<char> data(pos, pos + chunkPoints * pointSize);

        auto compressed(Compression::compress(data, schema()));
        output.put(std::to_string(id), *compressed);
    }

    m_chunk.reset(0);
}

} // namespace entwine

