/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/hierarchy.hpp>

#include <entwine/tree/cold.hpp>
#include <entwine/tree/heuristics.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/env.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const bool shallow(
            env("TESTING_SHALLOW") &&
            *env("TESTING_SHALLOW") == "true");
}

Hierarchy::Hierarchy(
        HierarchyCell::Pool& pool,
        const Metadata& metadata,
        const arbiter::Endpoint& ep,
        const arbiter::Endpoint* out,
        const bool exists,
        const bool readOnly)
    : Splitter(metadata.hierarchyStructure())
    , m_pool(pool)
    , m_metadata(metadata)
    , m_bounds(metadata.boundsNativeCubic())
    , m_structure(metadata.hierarchyStructure())
    , m_endpoint(ep.getSubEndpoint("h"))
    , m_outpoint(out ?
            makeUnique<arbiter::Endpoint>(out->getSubEndpoint("h")) :
            nullptr)
    , m_readOnly(readOnly)
{
    m_base.exists = true;

    if (!exists)
    {
        m_base.t = HierarchyBlock::create(
                m_pool,
                m_metadata,
                0,
                m_outpoint.get(),
                m_structure.baseIndexSpan());
    }
    else
    {
        m_base.t = HierarchyBlock::create(
                m_pool,
                m_metadata,
                0,
                m_outpoint.get(),
                m_structure.baseIndexSpan(),
                m_endpoint.getBinary("0" + metadata.postfix()));
    }

    if (exists)
    {
        const auto ids(
                extractIds(m_endpoint.get("ids" + m_metadata.postfix())));

        for (const auto& id : ids)
        {
            const ChunkInfo chunkInfo(m_structure.getInfo(id));
            const std::size_t chunkNum(chunkInfo.chunkNum());

            mark(id, chunkNum);
        }
    }
}

HierarchyCell& Hierarchy::count(const PointState& pointState, const int delta)
{
    if (m_structure.isWithinBase(pointState.depth()))
    {
        return m_base.t->count(pointState.index(), pointState.tick(), delta);
    }
    else
    {
        auto& slot(getOrCreate(pointState.chunkId(), pointState.chunkNum()));
        std::unique_ptr<HierarchyBlock>& block(slot.t);

        SpinGuard lock(slot.spinner);
        if (!block)
        {
            if (slot.exists)
            {
                block = HierarchyBlock::create(
                        m_pool,
                        m_metadata,
                        pointState.chunkId(),
                        m_outpoint.get(),
                        pointState.pointsPerChunk(),
                        m_endpoint.getBinary(
                            pointState.chunkId().str() + m_metadata.postfix()));
            }
            else
            {
                slot.exists = true;
                block = HierarchyBlock::create(
                        m_pool,
                        m_metadata,
                        pointState.chunkId(),
                        m_outpoint.get(),
                        pointState.pointsPerChunk());
            }
        }

        return block->count(pointState.index(), pointState.tick(), delta);
    }
}

HierarchyCell& Hierarchy::count(
        const ChunkInfo& chunkInfo,
        const std::size_t tick,
        const int delta)
{
    if (m_structure.isWithinBase(chunkInfo.depth()))
    {
        return m_base.t->count(chunkInfo.index(), tick, delta);
    }
    else
    {
        auto& slot(getOrCreate(chunkInfo.chunkId(), chunkInfo.chunkNum()));
        std::unique_ptr<HierarchyBlock>& block(slot.t);

        SpinGuard lock(slot.spinner);
        if (!block)
        {
            if (slot.exists)
            {
                block = HierarchyBlock::create(
                        m_pool,
                        m_metadata,
                        chunkInfo.chunkId(),
                        m_outpoint.get(),
                        chunkInfo.pointsPerChunk(),
                        m_endpoint.getBinary(
                            chunkInfo.chunkId().str() + m_metadata.postfix()));
            }
            else
            {
                slot.exists = true;
                block = HierarchyBlock::create(
                        m_pool,
                        m_metadata,
                        chunkInfo.chunkId(),
                        m_outpoint.get(),
                        chunkInfo.pointsPerChunk());
            }
        }

        return block->count(chunkInfo.index(), tick, delta);
    }
}

uint64_t Hierarchy::tryGet(const PointState& s) const
{
    if (const Slot* slotPtr = tryGet(s.chunkId(), s.chunkNum(), s.depth()))
    {
        const Slot& slot(*slotPtr);
        std::unique_ptr<HierarchyBlock>& block(slot.t);

        if (!slot.exists) return 0;

        SpinGuard lock(slot.spinner);
        if (!block)
        {
            std::cout <<
                "\tHierarchy awaken: " << s.chunkId() <<
                " at depth " << s.depth() <<
                ", fast? " << (s.chunkNum() < m_fast.size()) << std::endl;

            block = HierarchyBlock::create(
                    m_pool,
                    m_metadata,
                    s.chunkId(),
                    m_outpoint.get(),
                    s.pointsPerChunk(),
                    m_endpoint.getBinary(
                        s.chunkId().str() + m_metadata.postfix()),
                    m_readOnly);

            if (!block)
            {
                throw std::runtime_error("Failed awaken " + s.chunkId().str());
            }
        }

        return block->get(s.index(), s.tick());
    }

    return 0;
}

void Hierarchy::save(Pool& pool) const
{
    if (!m_outpoint) return;

    const std::string topPostfix(m_metadata.postfix());
    m_base.t->save(*m_outpoint, topPostfix);

    const std::string coldPostfix(m_metadata.postfix(true));
    iterateCold([this, &coldPostfix](
                const Id& chunkId,
                std::size_t num,
                const Slot& slot)
    {
        if (slot.t) slot.t->save(*m_outpoint, coldPostfix);
    }, &pool);

    Json::Value json;
    for (const auto& id : ids()) json.append(id.str());
    io::ensurePut(*m_outpoint, "ids" + topPostfix, toFastString(json));
}

/*
void Hierarchy::awakenAll(Pool& pool) const
{
    iterateCold([this](const Id chunkId, std::size_t num, const Slot& slot)
    {
        slot.t = HierarchyBlock::create(
                m_pool,
                m_metadata,
                chunkId,
                m_outpoint.get(),
                m_structure.getInfo(chunkId).pointsPerChunk(),
                m_endpoint.getBinary(chunkId.str() + m_metadata.postfix(true)));
    }, &pool);
}
*/

void Hierarchy::merge(Hierarchy& other, Pool& pool)
{
    Splitter::merge(
            dynamic_cast<BaseBlock&>(*m_base.t).merge(
                dynamic_cast<BaseBlock&>(*other.m_base.t)));

    Splitter::merge(other.ids());
}

Structure Hierarchy::structure(
        const Structure& treeStructure,
        const Subset* subset)
{
    const std::size_t minStartDepth(shallow ? 4 : 6);
    const std::size_t minBaseDepth(shallow ? 6 : 12);
    const std::size_t pointsPerChunk(treeStructure.basePointsPerChunk());

    const std::size_t startDepth(
            std::max(minStartDepth, treeStructure.baseDepthBegin()));

    const std::size_t nullDepth(0);

    const std::size_t baseDepth(
            std::max<std::size_t>(
                minBaseDepth,
                subset ? subset->minimumBaseDepth(pointsPerChunk) : 0));

    const std::size_t bumpDepth(baseDepth > minBaseDepth ? minBaseDepth : 0);
    const std::size_t coldDepth(0);

    const std::size_t dimensions(treeStructure.dimensions());
    const std::size_t numPointsHint(treeStructure.numPointsHint());
    const bool tubular(treeStructure.tubular());
    const bool dynamicChunks(true);
    const bool prefixIds(false);

    // Aside from the base, every block is mapped.
    const std::size_t mappedDepth(1);

    const std::size_t sparseDepth(
            std::ceil(
                static_cast<float>(treeStructure.sparseDepthBegin()) *
                heuristics::hierarchySparseFactor));

    return Structure(
            nullDepth,
            baseDepth,
            coldDepth,
            pointsPerChunk,
            dimensions,
            numPointsHint,
            tubular,
            dynamicChunks,
            prefixIds,
            mappedDepth,
            startDepth,
            sparseDepth,
            bumpDepth);
}

} // namespace entwine

