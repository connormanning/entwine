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

Entry& BaseBranch::getEntry(const std::size_t index)
{
    return m_chunk->getEntry(index);
}

void BaseBranch::saveImpl(Json::Value& meta)
{
    m_chunk->save(m_source);
}

void BaseBranch::load(const Json::Value& meta)
{
    if (m_ids.size() != 1)
    {
        throw std::runtime_error("Invalid serialized base branch.");
    }

    std::vector<char> compressed(m_source.get(std::to_string(indexBegin())));

    m_chunk.reset(new Chunk(schema(), indexBegin(), indexSpan(), compressed));
}

void BaseBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    std::mutex mutex;
    m_chunk->finalize(output, ids, mutex, start, chunkPoints);
    m_chunk.reset(0);
}

} // namespace entwine

