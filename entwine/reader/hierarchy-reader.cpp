/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/hierarchy-reader.hpp>

#include <entwine/reader/cache.hpp>
#include <entwine/tree/climber.hpp>

namespace entwine
{

namespace
{
    const std::string countKey("n");
}

Json::Value HierarchyReader::query(
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    const float available(m_pool.available());
    const float allocated(m_pool.allocated());

    std::cout << "HP: " <<
        (int)(allocated - available) << " / " << (int)allocated << ": " <<
        (allocated - available) / allocated * 100.0 << "%" <<
        std::endl;

    if (depthEnd < depthBegin)
    {
        throw std::runtime_error("Invalid range");
    }

    if (depthBegin < m_structure.baseDepthBegin())
    {
        throw std::runtime_error(
                "Request was less than hierarchy base depth");
    }

    // To get rid of any possible floating point mismatches, grow the bounds by
    // a bit and only include nodes that are entirely encapsulated by the
    // queryBounds.  Also normalize depths to match our internal mapping.
    Query query(
            queryBounds.growBy(.01),
            depthBegin - m_structure.startDepth(),
            depthEnd - m_structure.startDepth());

    PointState pointState(m_structure, m_bounds, m_structure.startDepth());
    std::deque<Dir> lag;

    Json::Value json;

    std::lock_guard<std::mutex> lock(m_mutex);
    Reservation reservation(m_cache, m_endpoint.prefixedRoot());
    traverse(json, reservation, query, pointState, lag);
    return json;
}

Json::Value HierarchyReader::queryVertical(
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    Json::Value tree(query(queryBounds, depthBegin, depthEnd));
    std::vector<std::size_t> out;
    reduce(out, 0, tree);

    Json::Value vert;
    for (const auto n : out) vert.append(static_cast<Json::UInt64>(n));
    return vert;
}

void HierarchyReader::traverse(
        Json::Value& json,
        Reservation& res,
        const HierarchyReader::Query& query,
        const PointState& pointState,
        std::deque<Dir>& lag)
{
    maybeReserve(res, pointState);

    const uint64_t inc(tryGet(pointState));
    if (!inc) return;

    if (pointState.depth() < query.depthBegin())
    {
        if (query.bounds().contains(pointState.bounds()))
        {
            // We've arrived at our query bounds.  All subsequent calls will
            // capture all children.
            //
            // In this case, the query base depth is higher than ours, meaning
            // we'll need to aggregate multiple nodes into a single node to
            // match the reference frame of the incoming query.
            for (std::size_t i(0); i < dirEnd(); ++i)
            {
                const Dir dir(toDir(i));
                auto curlag(lag);
                curlag.push_back(dir);
                traverse(json, res, query, pointState.getClimb(dir), curlag);
            }
        }
        else
        {
            // Query bounds is smaller than our current position's bounds, so
            // we need to split our bounds in a single direction.
            const Dir dir(
                    getDirection(
                        pointState.bounds().mid(),
                        query.bounds().mid()));

            traverse(json, res, query, pointState.getClimb(dir), lag);
        }
    }
    else if (
            query.bounds().contains(pointState.bounds()) &&
            pointState.depth() < query.depthEnd())
    {
        accumulate(json, res, query, pointState, lag, inc);
    }
}

void HierarchyReader::accumulate(
        Json::Value& json,
        Reservation& res,
        const HierarchyReader::Query& query,
        const PointState& pointState,
        std::deque<Dir>& lag,
        uint64_t inc)
{
    // Caller should not call if inc == 0 to avoid creating null keys.
    maybeReserve(res, pointState);
    json[countKey] = json[countKey].asUInt64() + inc;

    if (pointState.depth() + 1 >= query.depthEnd()) return;

    if (lag.empty())
    {
        for (std::size_t i(0); i < dirEnd(); ++i)
        {
            const Dir dir(toDir(i));
            const PointState nextState(pointState.getClimb(dir));

            if (const uint64_t inc = tryGet(nextState))
            {
                Json::Value& nextJson(json[dirToString(dir)]);
                accumulate(nextJson, res, query, nextState, lag, inc);
            }
        }
    }
    else
    {
        const Dir lagdir(lag.front());
        lag.pop_front();

        // Don't traverse into lagdir until we've confirmed that a child exists.
        Json::Value* nextJson(nullptr);

        for (std::size_t i(0); i < dirEnd(); ++i)
        {
            const Dir curdir(toDir(i));
            const PointState nextState(pointState.getClimb(curdir));

            if (const uint64_t inc = tryGet(nextState))
            {
                if (!nextJson) nextJson = &json[dirToString(lagdir)];
                auto curlag(lag);
                curlag.push_back(curdir);

                accumulate(*nextJson, res, query, nextState, curlag, inc);
            }
        }
    }
}

void HierarchyReader::reduce(
        std::vector<std::size_t>& out,
        std::size_t depth,
        const Json::Value& in) const
{
    if (const std::size_t n = in[countKey].asUInt64())
    {
        if (out.size() <= depth) out.resize(depth + 1, 0);
        out.at(depth) += n;

        ++depth;

        for (const auto& k : in.getMemberNames())
        {
            if (k != countKey) reduce(out, depth, in[k]);
        }
    }
}

void HierarchyReader::maybeReserve(
        Reservation& reservation,
        const PointState& pointState) const
{
    if (!isWithinBase(pointState.depth()))
    {
        if (const Slot* slot = tryGet(
                    pointState.chunkId(),
                    pointState.chunkNum(),
                    pointState.depth()))
        {
            if (slot->exists) reservation.insert(slot);
        }
    }
}

HierarchyReader::Reservation::~Reservation()
{
    if (!m_slots.empty()) m_cache.unrefHierarchy(m_path, m_slots);
}

void HierarchyReader::Reservation::insert(const Slot* slot)
{
    auto it(m_slots.find(slot));
    if (it == m_slots.end())
    {
        m_slots.insert(slot);
        m_cache.refHierarchySlot(m_path, slot);
    }
}

} // namespace entwine

