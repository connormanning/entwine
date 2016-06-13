/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/climber.hpp>
#include <entwine/tree/hierarchy.hpp>

namespace entwine
{

namespace
{
    const std::string countKey("n");
}

uint64_t HierarchyBlock::get(const PointState& pointState) const
{
    return get(pointState.index(), pointState.tick());
}

void Hierarchy::count(const PointState& state, const int delta)
{
    if (m_structure.isWithinBase(state.depth()))
    {
        m_base.count(state.index(), state.tick(), delta);
    }
    else
    {
        std::cout << state.depth() << std::endl;
        std::cout << "D: " << state.chunkNum() << std::endl;
    }
}

uint64_t Hierarchy::get(const PointState& pointState) const
{
    // TODO Check the actual block.
    return m_base.get(pointState);
}

Json::Value Hierarchy::query(
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    // TODO Should add startDepth to Metadata (not Structure - as that would
    // complicate the Hierarchy query Climber vs the building Climber).
    if (depthBegin < Hierarchy::startDepth())
    {
        throw std::runtime_error(
                "Request was less than hierarchy base depth");
    }

    Json::Value json;

    // To get rid of any possible floating point mismatches, grow the box by a
    // bit and only include nodes that are entirely encapsulated by the qbox.
    // Also normalize depths to match our internal mapping.
    Query query(
            qbox.growBy(.01),
            depthBegin - Hierarchy::startDepth(),
            depthEnd - Hierarchy::startDepth());

    PointState pointState(m_structure, m_bbox);
    std::deque<Dir> lag;

    traverse(json, query, pointState, lag);

    return json;
}

void Hierarchy::traverse(
        Json::Value& json,
        const Hierarchy::Query& query,
        const PointState& pointState,
        std::deque<Dir>& lag)
{
    const uint64_t inc(get(pointState));
    if (!inc) return;

    if (pointState.depth() < query.depthBegin())
    {
        if (query.bbox().contains(pointState.bbox()))
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
                traverse(json, query, pointState.getClimb(dir), curlag);
            }
        }
        else
        {
            // Query bounds is smaller than our current position's bounds, so
            // we need to split our bounds in a single direction.
            const Dir dir(
                    getDirection(query.bbox().mid(), pointState.bbox().mid()));

            traverse(json, query, pointState.getClimb(dir), lag);
        }
    }
    else if (
            query.bbox().contains(pointState.bbox()) &&
            pointState.depth() < query.depthEnd())
    {
        accumulate(json, query, pointState, lag, inc);
    }
}

void Hierarchy::accumulate(
        Json::Value& json,
        const Hierarchy::Query& query,
        const PointState& pointState,
        std::deque<Dir>& lag,
        uint64_t inc)
{
    // Caller should not call if inc == 0 to avoid creating null keys.
    json[countKey] = json[countKey].asUInt64() + inc;

    if (pointState.depth() + 1 >= query.depthEnd()) return;

    if (lag.empty())
    {
        for (std::size_t i(0); i < dirEnd(); ++i)
        {
            const Dir dir(toDir(i));
            const PointState nextState(pointState.getClimb(dir));

            if (const uint64_t inc = get(nextState))
            {
                accumulate(json[dirToString(dir)], query, nextState, lag, inc);
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

            if (const uint64_t inc = get(nextState))
            {
                if (!nextJson) nextJson = &json[dirToString(lagdir)];
                auto curlag(lag);
                curlag.push_back(curdir);

                accumulate(*nextJson, query, nextState, curlag, inc);
            }
        }

        lag.pop_back();
    }
}

void OHierarchy::accumulate(
        Node& out,
        std::deque<Dir>& lag,
        Node& cur,
        const std::size_t depth,
        const std::size_t depthEnd,
        const Id& id)
{
    out.incrementBy(cur.count());

    const std::size_t nextDepth(depth + 1);
    if (nextDepth >= depthEnd) return;

    if (lag.empty())
    {
        auto addChild(
        [this, &cur, nextDepth, depthEnd, &id]
        (Node& out, std::deque<Dir>& lag, Dir dir)
        {
            if (Node* node = cur.maybeNext(dir))
            {
                const Id childId(OHierarchy::climb(id, dir));

                if (m_step && (nextDepth - m_depthBegin) % m_step == 0)
                {
                    awaken(childId, node);
                }

                accumulate(
                    out.next(dir, m_nodePool),
                    lag,
                    *node,
                    nextDepth,
                    depthEnd,
                    childId);
            }
        });

        for (std::size_t i(0); i < 8; ++i) addChild(out, lag, toDir(i));
    }
    else
    {
        const Dir lagdir(lag.front());
        lag.pop_front();

        Node* nextNode(nullptr);

        auto addChild(
            [this, lagdir, &nextNode, &lag, &cur, nextDepth, depthEnd, &id]
            (Node& out, Dir curdir)
        {
            if (Node* node = cur.maybeNext(curdir))
            {
                const Id childId(OHierarchy::climb(id, curdir));

                if (m_step && (nextDepth - m_depthBegin) % m_step == 0)
                {
                    awaken(childId, node);
                }

                if (!nextNode) nextNode = &out.next(lagdir, m_nodePool);
                auto curlag(lag);
                curlag.push_back(curdir);

                accumulate(
                    *nextNode,
                    curlag,
                    *node,
                    nextDepth,
                    depthEnd,
                    childId);
            }
        });

        for (std::size_t i(0); i < 8; ++i) addChild(out, toDir(i));

        lag.pop_back();
    }
}





















































































Node::Node(
        Node::NodePool& nodePool,
        const char*& pos,
        const std::size_t step,
        NodeMap& edges,
        const Id id,
        std::size_t depth)
    : m_count()
    , m_children()
{
    assign(nodePool, pos, step, edges, id, depth);
}

void Node::assign(
        Node::NodePool& nodePool,
        const char*& pos,
        const std::size_t step,
        NodeMap& edges,
        const Id& id,
        std::size_t depth)
{
    std::copy(pos, pos + sizeof(uint64_t), reinterpret_cast<char*>(&m_count));
    pos += sizeof(uint64_t);

    const uint8_t mask(*pos);
    ++pos;

    if (mask)
    {
        ++depth;
        const bool exists(!step || depth % step);

        Dir dir;
        Id nextId;

        for (std::size_t i(0); i < 8; ++i)
        {
            if (mask & (1 << i))
            {
                dir = toDir(i);
                nextId = OHierarchy::climb(id, dir);

                if (exists)
                {
                    m_children.emplace(
                            std::make_pair(
                                dir,
                                nodePool.acquireOne(
                                    nodePool,
                                    pos,
                                    step,
                                    edges,
                                    nextId,
                                    depth)));;
                }
                else
                {
                    auto it(
                            m_children.insert(
                                std::make_pair(dir, nodePool.acquireOne())));

                    edges[nextId] = &*it.first->second;
                }
            }
        }
    }
}

void Node::merge(Node& other)
{
    m_count += other.count();

    for (auto& theirs : other.children())
    {
        const Dir dir(theirs.first);;

        if (m_children.count(dir))
        {
            m_children.at(dir)->merge(*theirs.second);
        }
        else
        {
            m_children.emplace(std::make_pair(dir, std::move(theirs.second)));
        }
    }
}

void Node::insertInto(Json::Value& json) const
{
    json[countKey] = static_cast<Json::UInt64>(m_count);

    if (m_count)
    {
        for (const auto& c : m_children)
        {
            c.second->insertInto(json[dirToString(c.first)]);
        }
    }
}

Node::NodeSet Node::insertInto(
        const arbiter::Endpoint& ep,
        const std::string postfix,
        const std::size_t step)
{
    NodeSet anchors;

    AnchoredMap slice;
    slice[0] = AnchoredNode(this);

    while (slice.size()) slice = insertSlice(anchors, slice, ep, postfix, step);

    return anchors;
}

Node::AnchoredMap Node::insertSlice(
        NodeSet& anchors,
        const AnchoredMap& slice,
        const arbiter::Endpoint& ep,
        const std::string postfix,
        const std::size_t step)
{
    std::vector<char> data;
    AnchoredMap fullSlice;
    AnchoredMap nextSlice;
    Id anchor(slice.begin()->first);

    auto write([&]()
    {
        anchors.insert(anchor);
        const std::string path(anchor.str() + (anchor.zero() ? postfix : ""));
        ep.put(path, data);
        data.clear();

        if (nextSlice.size())
        {
            nextSlice.begin()->second.isAnchor = true;
            fullSlice.insert(nextSlice.begin(), nextSlice.end());
            nextSlice.clear();
        }
    });

    for (const auto& n : slice)
    {
        if (
                (data.size() && n.second.isAnchor) ||
                (data.size() > OHierarchy::defaultChunkBytes))
        {
            if (n.second.isAnchor)
            {
                std::cout << "Anchoring " << n.first << std::endl;
            }

            write();

            anchor = n.first;
        }

        n.second.node->insertData(data, nextSlice, n.first, step);
    }

    if (data.size()) write();

    return fullSlice;
}

void Node::insertData(
        std::vector<char>& data,
        AnchoredMap& nextSlice,
        const Id& id,
        const std::size_t step,
        std::size_t depth)
{
    if (insertBinary(data))
    {
        if (!step || ++depth % step)
        {
            for (auto& c : m_children)
            {
                c.second->insertData(
                        data,
                        nextSlice,
                        OHierarchy::climb(id, c.first),
                        step,
                        depth);
            }
        }
        else
        {
            for (auto& c : m_children)
            {
                nextSlice.insert(
                        nextSlice.end(),
                        std::make_pair(
                            OHierarchy::climb(id, c.first),
                            AnchoredNode(&*c.second)));
            }
        }
    }
}

bool Node::insertBinary(std::vector<char>& s) const
{
    s.insert(
            s.end(),
            reinterpret_cast<const char*>(&m_count),
            reinterpret_cast<const char*>(&m_count + 1));

    uint8_t mask(0);

    if (m_count)
    {
        for (const auto& c : m_children)
        {
            mask |= (1 << static_cast<int>(c.first));
        }
    }

    s.push_back(mask);

    return m_count;
}

OHierarchy::OHierarchy(
        const BBox& bbox,
        Node::NodePool& nodePool,
        const Json::Value& json,
        const arbiter::Endpoint& ep,
        const std::string postfix)
    : m_bbox(bbox)
    , m_nodePool(nodePool)
    , m_depthBegin(json["depthBegin"].asUInt64())
    , m_step(json["step"].asUInt64())
    , m_root()
    , m_edges()
    , m_anchors()
    , m_mutex()
    , m_endpoint(new arbiter::Endpoint(ep))
    , m_postfix(postfix)
{
    const auto bin(ep.tryGetBinary("0" + postfix));

    if (bin && bin->size())
    {
        const char* pos(bin->data());
        m_root = Node(nodePool, pos, m_step, m_edges);

        if (m_step)
        {
            Json::Reader reader;
            Json::Value anchorsJson;

            const std::string anchorsData(ep.get("anchors" + postfix));
            if (!reader.parse(anchorsData, anchorsJson, false))
            {
                throw std::runtime_error(
                        "Anchor parse error: " +
                        reader.getFormattedErrorMessages());
            }

            for (Json::ArrayIndex i(0); i < anchorsJson.size(); ++i)
            {
                m_anchors.insert(Id(anchorsJson[i].asString()));
            }
        }
    }
    else
    {
        std::cout << "No Ohierarchy data found" << std::endl;
    }
}

void OHierarchy::awaken(const Id& id, const Node* node)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (node && node->count()) return;

    auto lowerAnchor(m_anchors.lower_bound(id));

    if (lowerAnchor == m_anchors.end())
    {
        lowerAnchor = m_anchors.find(*m_anchors.rbegin());
    }
    else if (*lowerAnchor != id)
    {
        // If the lower anchor is not exactly equal to the Id we're looking for,
        // then it currently points to the next anchor *greater* than this Id.
        // So decrement it by one.
        if (*lowerAnchor != *m_anchors.begin()) --lowerAnchor;
        else
        {
            std::cout <<
                ("Invalid lower anchor " + lowerAnchor->str() + " for " +
                id.str()) << std::endl;

            throw std::runtime_error(
                "Invalid lower anchor " + lowerAnchor->str() + " for " +
                id.str());
        }
    }

    if (m_awoken.count(*lowerAnchor))
    {
        std::cout <<
                ("REAWAKENING " + lowerAnchor->str() + " for " + id.str()) <<
                std::endl;
    }
    else
    {
        std::cout << "Awakening " << *lowerAnchor << std::endl;
        m_awoken.insert(*lowerAnchor);
    }

    auto it(m_edges.find(*lowerAnchor));
    if (it == m_edges.end())
    {
        std::cout << ("No edge for lower anchor " + lowerAnchor->str()) <<
            std::endl;
        throw std::runtime_error(
                "No edge for lower anchor " + lowerAnchor->str());
    }

    const auto upperAnchor(m_anchors.upper_bound(id));
    const Id edgeEnd(upperAnchor != m_anchors.end() ? *upperAnchor : 0);

    const std::vector<char> bin(
            m_endpoint->getBinary(lowerAnchor->str() + m_postfix));

    const char* pos(bin.data());

    Node::NodeMap newEdges;

    while (it != m_edges.end() && (edgeEnd.zero() || it->first < edgeEnd))
    {
        it->second->assign(m_nodePool, pos, m_step, newEdges, it->first);
        it = m_edges.erase(it);
    }

    m_edges.insert(newEdges.begin(), newEdges.end());
}

Json::Value OHierarchy::toJson(const arbiter::Endpoint& ep, std::string postfix)
{
    // Postfixing is only applied to the anchors file and the base anchor.
    const Node::NodeSet newAnchors(m_root.insertInto(ep, postfix, m_step));
    m_anchors.insert(newAnchors.begin(), newAnchors.end());

    Json::Value json;
    json["depthBegin"] = static_cast<Json::UInt64>(m_depthBegin);
    json["step"] = static_cast<Json::UInt64>(m_step);

    Json::Value jsonAnchors;
    for (const auto& a : m_anchors)
    {
        if (!a.zero()) jsonAnchors.append(a.str());
    }

    if (jsonAnchors.empty()) jsonAnchors.resize(0);

    ep.put("anchors" + postfix, jsonAnchors.toStyledString());

    return json;
}

Json::Value OHierarchy::query(
        BBox qbox,
        const std::size_t qDepthBegin,
        const std::size_t qDepthEnd)
{
    if (qDepthBegin < m_depthBegin)
    {
        throw std::runtime_error(
                "Request was less than hierarchy base depth");
    }

    // To get rid of any possible floating point mismatches, grow the box by a
    // bit and only include nodes that are entirely encapsulated by the qbox.
    qbox = qbox.growBy(.01);

    Node node;
    std::deque<Dir> lag;

    {
        traverse(
                node,
                lag,
                m_root,
                m_bbox,
                qbox,
                m_depthBegin,
                qDepthBegin,
                qDepthEnd);
    }

    Json::Value json;
    node.insertInto(json);
    return json;
}

void OHierarchy::traverse(
        Node& out,
        std::deque<Dir>& lag,
        Node& cur,
        const BBox& cbox,   // Current bbox.
        const BBox& qbox,   // Query bbox.
        const std::size_t depth,
        const std::size_t db,
        const std::size_t de,
        const Id id)
{
    if (depth < db)
    {
        // Not adding results yet, just traversing.
        const std::size_t next(depth + 1);

        if (qbox.contains(cbox))
        {
            auto addChild(
                [this, &lag, &cur, cbox, qbox, next, db, de, &id]
                (Node& out, Dir dir)
            {
                if (Node* node = cur.maybeNext(dir))
                {
                    const Id childId(OHierarchy::climb(id, dir));

                    if (m_step && (next - m_depthBegin) % m_step == 0)
                    {
                        awaken(childId, node);
                    }

                    auto curlag(lag);
                    curlag.push_back(dir);
                    traverse(
                        out,
                        curlag,
                        *node,
                        cbox.get(dir),
                        qbox,
                        next,
                        db,
                        de,
                        childId);
                }
            });

            // We've arrived at our query bounds.  All subsequent calls will
            // capture all children.
            for (std::size_t i(0); i < 8; ++i)
            {
                addChild(out, static_cast<Dir>(i));
            }
        }
        else
        {
            // Query bounds is smaller than our current position's bounds, so
            // we need to split our bounds in a single direction.
            const Dir dir(getDirection(qbox.mid(), cbox.mid()));

            if (Node* node = cur.maybeNext(dir))
            {
                const Id childId(OHierarchy::climb(id, dir));

                if (m_step && (next - m_depthBegin) % m_step == 0)
                {
                    awaken(childId, node);
                }

                const BBox nbox(cbox.get(dir));
                traverse(out, lag, *node, nbox, qbox, next, db, de, childId);
            }
        }
    }
    else if (qbox.contains(cbox) && depth < de) // User error if not.
    {
        accumulate(out, lag, cur, depth, de, id);
    }
}

} // namespace entwine

