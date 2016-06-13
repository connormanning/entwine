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
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

Registry::Registry(const Builder& builder, const bool exists)
    : m_builder(builder)
    , m_structure(m_builder.metadata().structure())
    , m_base()
    , m_cold()
{
    const Metadata& metadata(m_builder.metadata());

    if (m_structure.baseIndexSpan())
    {
        if (!exists)
        {
            m_base.reset(
                    static_cast<BaseChunk*>(
                        Chunk::create(
                            m_builder,
                            0,
                            m_structure.baseIndexBegin(),
                            m_structure.baseIndexSpan()).release()));
        }
        else
        {
            const std::string basePath(
                    m_structure.baseIndexBegin().str() + metadata.postfix());

            if (auto data = m_builder.outEndpoint().tryGetBinary(basePath))
            {
                m_base.reset(
                        static_cast<BaseChunk*>(
                            Chunk::create(
                                m_builder,
                                0,
                                m_structure.baseIndexBegin(),
                                m_structure.baseIndexSpan(),
                                std::move(data)).release()));
            }
            else
            {
                throw std::runtime_error("No base data found");
            }
        }
    }

    if (m_structure.hasCold())
    {
        m_cold.reset(new Cold(m_builder, exists));
    }
}

Registry::~Registry()
{ }

void Registry::save() const
{
    if (m_cold) m_cold->save(m_builder.outEndpoint());
}

bool Registry::addPoint(
        Cell::PooledNode& cell,
        Climber& climber,
        Clipper& clipper,
        const std::size_t maxDepth)
{
    Tube::Insertion attempt;

    while (true)
    {
        attempt = insert(climber, clipper, cell);

        if (!attempt.done())
        {
            if (attempt.delta()) climber.count(attempt.delta());

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

std::set<Id> Registry::ids() const
{
    if (m_cold) return m_cold->ids();
    else return std::set<Id>();
}

} // namespace entwine

