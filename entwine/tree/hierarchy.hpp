/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <entwine/third/json/json.hpp>
#include <entwine/tree/hierarchy-block.hpp>
#include <entwine/tree/splitter.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class PointState;

class Hierarchy : Splitter<HierarchyBlock>
{
public:
    Hierarchy(
            const Metadata& metadata,
            const arbiter::Endpoint& ep,
            bool exists);

    using Splitter::tryGet;

    void count(const PointState& state, int delta);
    uint64_t tryGet(const PointState& pointState) const;

    void save(const arbiter::Endpoint& ep)
    {
        const std::string postfix(m_metadata.postfix());

        m_base.t->save(ep, postfix);

        for (std::size_t i(0); i < m_fast.size(); ++i)
        {
            if (m_fast[i].mark)
            {
                m_fast[i].t->save(ep, postfix);
            }
        }

        for (auto& pair : m_slow) pair.second.t->save(ep, postfix);

        Json::Value json;
        for (const auto& id : ids()) json.append(id.str());
        ep.put("ids" + postfix, toFastString(json));
    }

    void awakenAll() { }    // TODO.
    void merge(const Hierarchy& other) { }  // TODO.

    Json::Value query(
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    static Structure structure(const Structure& treeStructure);

private:
    class Query
    {
    public:
        Query(const BBox& bbox, std::size_t depthBegin, std::size_t depthEnd)
            : m_bbox(bbox), m_depthBegin(depthBegin), m_depthEnd(depthEnd)
        { }

        const BBox& bbox() const { return m_bbox; }
        std::size_t depthBegin() const { return m_depthBegin; }
        std::size_t depthEnd() const { return m_depthEnd; }

    private:
        const BBox m_bbox;
        const std::size_t m_depthBegin;
        const std::size_t m_depthEnd;
    };

    void traverse(
            Json::Value& json,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag);

    void accumulate(
            Json::Value& json,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag,
            uint64_t inc);

    const Metadata& m_metadata;
    const BBox& m_bbox;
    const Structure& m_structure;
    const arbiter::Endpoint m_endpoint;
};
















































class Node
{
public:
    typedef splicer::ObjectPool<Node> NodePool;
    typedef NodePool::UniqueNodeType PooledNode;

    typedef std::map<Id, Node*> NodeMap;
    typedef std::set<Id> NodeSet;
    typedef std::map<Dir, PooledNode> Children;

    struct AnchoredNode
    {
        AnchoredNode() : node(nullptr), isAnchor(false) { }
        explicit AnchoredNode(Node* node) : node(node), isAnchor(false) { }

        Node* node;
        bool isAnchor;
    };

    typedef std::map<Id, AnchoredNode> AnchoredMap;

    Node() : m_count(0), m_children() { }

    Node(
            NodePool& nodePool,
            const char*& pos,
            std::size_t step,
            NodeMap& edges,
            Id id = 0,
            std::size_t depth = 0);

    void assign(
            NodePool& nodePool,
            const char*& pos,
            std::size_t step,
            NodeMap& edges,
            const Id& id,
            std::size_t depth = 0);

    Node& next(Dir dir, NodePool& nodePool)
    {
        if (Node* node = maybeNext(dir)) return *node;
        else
        {
            auto result(
                    m_children.emplace(
                        std::make_pair(dir, nodePool.acquireOne())));

            return *result.first->second;
        }
    }

    Node* maybeNext(Dir dir)
    {
        auto it(m_children.find(dir));

        if (it != m_children.end()) return &*it->second;
        else return nullptr;
    }

    void increment() { ++m_count; }
    void incrementBy(std::size_t n) { m_count += n; }

    std::size_t count() const { return m_count; }

    void merge(Node& other);
    void insertInto(Json::Value& json) const;

    NodeSet insertInto(
            const arbiter::Endpoint& ep,
            std::string postfix,
            std::size_t step);

    const Children& children() const { return m_children; }

private:
    Children& children() { return m_children; }

    AnchoredMap insertSlice(
            NodeSet& anchors,
            const AnchoredMap& slice,
            const arbiter::Endpoint& ep,
            std::string postfix,
            std::size_t step);

    void insertData(
            std::vector<char>& data,
            AnchoredMap& nextSlice,
            const Id& id,
            std::size_t step,
            std::size_t depth = 0);

    bool insertBinary(std::vector<char>& s) const;

    uint64_t m_count;
    Children m_children;
};

inline bool operator==(const Node& lhs, const Node& rhs)
{
    if (lhs.count() == rhs.count())
    {
        const auto& lhsChildren(lhs.children());
        const auto& rhsChildren(rhs.children());

        if (lhsChildren.size() == rhsChildren.size())
        {
            for (const auto& c : lhsChildren)
            {
                if (
                        !rhsChildren.count(c.first) ||
                        !(*c.second == *rhsChildren.at(c.first)))
                {
                    return false;
                }
            }

            return true;
        }
    }

    return false;
}

class OHierarchy
{
public:
    OHierarchy(const BBox& bbox, Node::NodePool& nodePool)
        : m_bbox(bbox)
        , m_nodePool(nodePool)
        , m_depthBegin(defaultDepthBegin)
        , m_step(defaultStep)
        , m_root()
        , m_edges()
        , m_anchors()
        , m_mutex()
        , m_endpoint()
        , m_postfix()
    { }

    OHierarchy(
            const BBox& bbox,
            Node::NodePool& nodePool,
            const Json::Value& json,
            const arbiter::Endpoint& ep,
            std::string postfix = "");

    Node& root() { return m_root; }

    Json::Value toJson(const arbiter::Endpoint& ep, std::string postfix);

    Json::Value query(
            BBox qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void merge(OHierarchy& other)
    {
        m_root.merge(other.root());
        m_anchors.insert(other.m_anchors.begin(), other.m_anchors.end());
    }

    std::size_t depthBegin() const { return m_depthBegin; }
    std::size_t step() const { return m_step; }
    const BBox& bbox() const { return m_bbox; }

    void awakenAll()
    {
        for (const auto& a : m_anchors) awaken(a);
        m_anchors.clear();
    }

    void setStep(std::size_t set) { m_step = set; }

    static const std::size_t defaultDepthBegin = 6;
    static const std::size_t defaultStep = 8;
    static const std::size_t defaultChunkBytes = 1 << 20;   // 1 MB.

    static Id climb(const Id& id, Dir dir)
    {
        return (id << 3) + 1 + toIntegral(dir);
    }

    Node::NodePool& nodePool() { return m_nodePool; }

private:
    OHierarchy(const OHierarchy& other) = delete;
    OHierarchy& operator=(const OHierarchy& other) = delete;

    void traverse(
            Node& out,
            std::deque<Dir>& lag,
            Node& cur,
            const BBox& cbox,
            const BBox& qbox,
            std::size_t depth,
            std::size_t depthBegin,
            std::size_t depthEnd,
            Id id = 0);

    void accumulate(
            Node& node,
            std::deque<Dir>& lag,
            Node& cur,
            std::size_t depth,
            std::size_t depthEnd,
            const Id& id);

    void awaken(const Id& id, const Node* node = nullptr);

    const BBox& m_bbox;
    Node::NodePool& m_nodePool;

    const std::size_t m_depthBegin;
    std::size_t m_step;

    Node m_root;
    Node::NodeMap m_edges;
    Node::NodeSet m_anchors;
    Node::NodeSet m_awoken;

    mutable std::mutex m_mutex;
    std::unique_ptr<arbiter::Endpoint> m_endpoint;
    std::string m_postfix;
};

class HierarchyClimber
{
public:
    HierarchyClimber(OHierarchy& hierarchy, std::size_t dimensions)
        : m_hierarchy(hierarchy)
        , m_bbox(hierarchy.bbox())
        , m_depthBegin(hierarchy.depthBegin())
        , m_depth(m_depthBegin)
        , m_step(hierarchy.step())
        , m_node(&hierarchy.root())
    { }

    void reset()
    {
        m_bbox = m_hierarchy.bbox();
        m_depth = m_depthBegin;
        m_node = &m_hierarchy.root();
    }

    void magnify(const Point& point)
    {
        const Dir dir(getDirection(point, m_bbox.mid()));
        m_bbox.go(dir);
        m_node = &m_node->next(dir, m_hierarchy.nodePool());
    }

    void count() { m_node->increment(); }
    std::size_t depthBegin() const { return m_depthBegin; }

private:
    OHierarchy& m_hierarchy;
    BBox m_bbox;

    const std::size_t m_depthBegin;

    std::size_t m_depth;
    std::size_t m_step;

    Node* m_node;
};

} // namespace entwine

