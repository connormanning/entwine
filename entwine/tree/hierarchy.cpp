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

namespace entwine
{

namespace
{
    const std::string countKey("n");
}

Node::Node(const Json::Value& json)
    : m_count(json[countKey].asUInt64())
    , m_children()
{
    if (!m_count) throw std::runtime_error("Invalid hierarchy count");

    for (const auto& key : json.getMemberNames())
    {
        if (key != countKey)
        {
            m_children[stringToDir(key)] = Node(json[key]);
        }
    }
}

void Node::merge(Node& other)
{
    m_count += other.count();

    for (auto& theirs : other.children())
    {
        auto& mine(m_children[theirs.first]);
        if (!mine.count())
        {
            std::swap(mine, theirs.second);
        }
        else
        {
            mine.merge(theirs.second);
        }
    }
}

void Node::insertInto(Json::Value& json) const
{
    if (m_count)
    {
        json[countKey] = static_cast<Json::UInt64>(m_count);
        for (const auto& c : m_children)
        {
            c.second.insertInto(json[dirToString(c.first)]);
        }
    }
}

Json::Value Hierarchy::query(
        BBox qbox,
        const std::size_t qDepthBegin,
        const std::size_t qDepthEnd) const
{
    if (qDepthBegin < m_depthBegin)
    {
        throw std::runtime_error(
                "Request was less than hierarchy base depth");
    }

    // To get rid of any possible floating point mismatches, grow the box by a
    // bit and only include nodes that are entirely encapsulated by the qbox.
    qbox.growBy(.01);

    Node node;
    std::deque<Dir> lag;
    traverse(
            node,
            lag,
            m_root,
            m_bbox,
            qbox,
            m_depthBegin,
            qDepthBegin,
            qDepthEnd);

    Json::Value json;
    node.insertInto(json);
    return json;
}

void Hierarchy::traverse(
        Node& out,
        std::deque<Dir>& lag,
        const Node& cur,
        const BBox& cbox,   // Current bbox.
        const BBox& qbox,   // Query bbox.
        const std::size_t depth,
        const std::size_t db,
        const std::size_t de) const
{
    if (depth < db)
    {
        // Not adding results yet, just traversing.
        const std::size_t next(depth + 1);

        if (qbox.contains(cbox))
        {
            auto addChild(
                [this, &lag, cur, cbox, qbox, next, db, de]
                (Node& out, Dir dir)
            {
                if (const Node* node = cur.maybeNext(dir))
                {
                    auto curlag(lag);
                    curlag.push_back(dir);
                    traverse(
                        out, curlag, *node, cbox.get(dir), qbox, next, db, de);
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

            if (const Node* node = cur.maybeNext(dir))
            {
                const BBox nbox(cbox.get(dir));
                traverse(out, lag, *node, nbox, qbox, next, db, de);
            }
        }
    }
    else if (qbox.contains(cbox) && depth < de) // User error if not.
    {
        accumulate(out, lag, cur, depth, de);
    }
}

void Hierarchy::accumulate(
        Node& out,
        std::deque<Dir>& lag,
        const Node& cur,
        std::size_t depth,
        const std::size_t depthEnd) const
{
    out.incrementBy(cur.count());

    const std::size_t nextDepth(depth + 1);
    if (nextDepth < depthEnd)
    {
        if (lag.empty())
        {
            auto addChild(
            [this, cur, nextDepth, depthEnd]
            (Node& out, std::deque<Dir>& lag, Dir dir)
            {
                if (const Node* node = cur.maybeNext(dir))
                {
                    accumulate(out.next(dir), lag, *node, nextDepth, depthEnd);
                }
            });

            for (std::size_t i(0); i < 8; ++i)
            {
                addChild(out, lag, static_cast<Dir>(i));
            }
        }
        else
        {
            const Dir lagdir(lag.front());
            lag.pop_front();

            Node* nextNode(nullptr);

            auto addChild(
                [this, lagdir, &nextNode, lag, cur, nextDepth, depthEnd]
                (Node& out, Dir curdir)
            {
                if (const Node* node = cur.maybeNext(curdir))
                {
                    if (!nextNode) nextNode = &out.next(lagdir);
                    auto curlag(lag);
                    curlag.push_back(curdir);
                    accumulate(*nextNode, curlag, *node, nextDepth, depthEnd);
                }
            });

            for (std::size_t i(0); i < 8; ++i)
            {
                addChild(out, static_cast<Dir>(i));
            }

            lag.pop_back();
        }
    }
}

} // namespace entwine

