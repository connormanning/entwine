/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/registry.hpp>

#include <algorithm>

#include <pdal/PointView.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

Registry::Registry(
        arbiter::Endpoint& endpoint,
        const Builder& builder,
        const std::size_t clipPoolSize)
    : m_endpoint(endpoint)
    , m_builder(builder)
    , m_structure(builder.structure())
    , m_discardDuplicates(m_structure.discardDuplicates())
    , m_as3d(m_structure.is3d() || m_structure.tubular())
    , m_base()
    , m_cold()
{
    if (m_structure.baseIndexSpan())
    {
        m_base.reset(
                static_cast<BaseChunk*>(
                    Chunk::create(
                        m_builder,
                        m_builder.bbox(),
                        0,
                        m_structure.baseIndexBegin(),
                        m_structure.baseIndexSpan()).release()));
    }

    if (m_structure.hasCold())
    {
        m_cold.reset(new Cold(endpoint, m_builder, clipPoolSize));
    }
}

Registry::Registry(
        arbiter::Endpoint& endpoint,
        const Builder& builder,
        const std::size_t clipPoolSize,
        const Json::Value& ids)
    : m_endpoint(endpoint)
    , m_builder(builder)
    , m_structure(builder.structure())
    , m_discardDuplicates(m_structure.discardDuplicates())
    , m_as3d(m_structure.is3d() || m_structure.tubular())
    , m_base()
    , m_cold()
{
    if (m_structure.baseIndexSpan())
    {
        const std::string basePath(
                m_structure.baseIndexBegin().str() +
                m_builder.postfix());

        std::unique_ptr<std::vector<char>> data(
                m_endpoint.tryGetSubpathBinary(basePath));

        if (data)
        {
            m_base.reset(
                    static_cast<BaseChunk*>(
                        Chunk::create(
                            m_builder,
                            m_builder.bbox(),
                            0,
                            m_structure.baseIndexBegin(),
                            m_structure.baseIndexSpan(),
                            std::move(data)).release()));
        }
        else
        {
            std::cout << "No base data found" << std::endl;
            m_base.reset(
                static_cast<BaseChunk*>(
                    Chunk::create(
                        m_builder,
                        m_builder.bbox(),
                        0,
                        m_structure.baseIndexBegin(),
                        m_structure.baseIndexSpan()).release()));
        }
    }

    if (m_structure.hasCold())
    {
        m_cold.reset(new Cold(endpoint, builder, clipPoolSize, ids));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(
        Cell::PooledNode& cell,
        Climber& climber,
        Clipper& clipper,
        const std::size_t maxDepth)
{
    while (true)
    {
        if (!insert(climber, clipper, cell))
        {
            if (
                    m_structure.inRange(climber.depth() + 1) &&
                    (!maxDepth || climber.depth() + 1 < maxDepth))
            {
                climber.magnify(cell->point());
            }
            else
            {
                return false;
            }
        }
        else
        {
            climber.count();
            return true;
        }
    }
}

bool Registry::insert(
        const Climber& climber,
        Clipper& clipper,
        Cell::PooledNode& cell)
{
    if (m_structure.isWithinBase(climber.depth()))
    {
        return m_base->insert(climber, cell);
    }
    else
    {
        return m_cold->insert(climber, clipper, cell);
    }
}

void Registry::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id)
{
    m_cold->clip(index, chunkNum, id);
}

void Registry::merge(const Registry& other)
{
    if (m_cold && other.m_cold) m_cold->merge(*other.m_cold);
    if (m_base && other.m_base) m_base->merge(*other.m_base);
}

Json::Value Registry::toJson() const
{
    if (m_cold) return m_cold->toJson();
    else return Json::Value();
}

std::set<Id> Registry::ids() const
{
    if (m_cold) return m_cold->ids();
    else return std::set<Id>();
}

} // namespace entwine

