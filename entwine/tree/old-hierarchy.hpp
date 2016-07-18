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

#include <set>

#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/defs.hpp>

namespace entwine
{

class Metadata;

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
    void insertInto(Hierarchy& modern, const Metadata& metadata) const;

    NodeSet insertInto(
            const arbiter::Endpoint& ep,
            std::string postfix,
            std::size_t step);

    const Children& children() const { return m_children; }

private:
    void insertInto(HierarchyState& state) const;

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

class OldHierarchy
{
public:
    OldHierarchy(
            const Json::Value& json,
            const arbiter::Endpoint& ep,
            std::string postfix = "");

    Node& root() { return m_root; }

    Json::Value toJson(const arbiter::Endpoint& ep, std::string postfix);
    void insertInto(Hierarchy& modern, const Metadata& metadata);

    Json::Value query(
            Bounds queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void merge(OldHierarchy& other)
    {
        m_root.merge(other.root());
        m_anchors.insert(other.m_anchors.begin(), other.m_anchors.end());
    }

    std::size_t depthBegin() const { return m_depthBegin; }
    std::size_t step() const { return m_step; }

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
    OldHierarchy(const OldHierarchy& other) = delete;
    OldHierarchy& operator=(const OldHierarchy& other) = delete;

    void traverse(
            Node& out,
            std::deque<Dir>& lag,
            Node& cur,
            const Bounds& currentBounds,
            const Bounds& queryBounds,
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

    Node::NodePool m_nodePool;

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

/*
class HierarchyClimber
{
public:
    HierarchyClimber(OldHierarchy& hierarchy, std::size_t dimensions)
        : m_hierarchy(hierarchy)
        , m_bounds(hierarchy.bounds())
        , m_depthBegin(hierarchy.depthBegin())
        , m_depth(m_depthBegin)
        , m_step(hierarchy.step())
        , m_node(&hierarchy.root())
    { }

    void reset()
    {
        m_bounds = m_hierarchy.bounds();
        m_depth = m_depthBegin;
        m_node = &m_hierarchy.root();
    }

    void magnify(const Point& point)
    {
        const Dir dir(getDirection(m_bounds.mid(), point));
        m_bounds.go(dir);
        m_node = &m_node->next(dir, m_hierarchy.nodePool());
    }

    void count() { m_node->increment(); }
    std::size_t depthBegin() const { return m_depthBegin; }

private:
    OldHierarchy& m_hierarchy;
    Bounds m_bounds;

    const std::size_t m_depthBegin;

    std::size_t m_depth;
    std::size_t m_step;

    Node* m_node;
};

class OldHierarchyState
{
public:
    OldHierarchyState(const Metadata& metadata, OldHierarchy* hierarchy);

    void climb(const Point& point)
    {
        if (m_climber && ++m_depth > m_climber->depthBegin())
        {
            m_climber->magnify(point);
        }
    }

    void reset() { if (m_climber) m_climber->reset(); }
    void count() { if (m_depth >= m_climber->depthBegin()) m_climber->count(); }

private:
    std::size_t m_depth;
    std::unique_ptr<HierarchyClimber> m_climber;
};
*/

} // namespace entwine

