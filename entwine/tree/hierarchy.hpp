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
#include <deque>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <entwine/third/json/json.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/bbox.hpp>

namespace entwine
{

class Node
{
public:
    typedef std::map<Id, Node*> NodeMap;
    typedef std::set<Id> NodeSet;
    typedef std::map<Dir, Node> Children;

    Node() : m_count(0), m_children() { }

    Node(
            const char*& pos,
            std::size_t step,
            NodeMap& edges,
            Id id = 0,
            std::size_t depth = 0);

    void assign(
            const char*& pos,
            std::size_t step,
            NodeMap& edges,
            const Id& id,
            std::size_t depth = 0);

    Node& next(Dir dir) { return m_children[dir]; }

    Node* maybeNext(Dir dir)
    {
        auto it(m_children.find(dir));

        if (it != m_children.end()) return &it->second;
        else return nullptr;
    }

    void increment() { ++m_count; }
    void incrementBy(std::size_t n) { m_count += n; }

    std::size_t count() const { return m_count; }

    void merge(Node& other);
    void insertInto(Json::Value& json) const;

    NodeSet insertInto(const arbiter::Endpoint& ep, std::size_t step);

    const Children& children() const { return m_children; }

private:
    Children& children() { return m_children; }

    NodeMap insertSlice(
            NodeSet& anchors,
            const NodeMap& slice,
            const arbiter::Endpoint& ep,
            std::size_t step);

    void insertData(
            std::vector<char>& data,
            NodeMap& nextSlice,
            const Id& id,
            std::size_t step,
            std::size_t depth = 0);

    bool insertBinary(std::vector<char>& s) const;

    uint64_t m_count;
    Children m_children;
};

class Hierarchy
{
public:
    Hierarchy(const BBox& bbox)
        : m_bbox(bbox)
        , m_depthBegin(defaultDepthBegin)
        , m_step(defaultStep)
        , m_root()
        , m_edges()
        , m_anchors()
        , m_endpoint()
    { }

    Hierarchy(
            const BBox& bbox,
            const Json::Value& json,
            const arbiter::Endpoint& ep);

    Node& root() { return m_root; }

    Json::Value toJson(const arbiter::Endpoint& ep, std::string postfix);

    Json::Value query(
            BBox qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void merge(Hierarchy& other)
    {
        m_root.merge(other.root());
    }

    std::size_t depthBegin() const { return m_depthBegin; }
    std::size_t step() const { return m_step; }
    const BBox& bbox() const { return m_bbox; }

    void awakenAll()
    {
        for (const auto& a : m_anchors) awaken(a);
    }

    void setStep(std::size_t set) { m_step = set; }

    static const std::size_t defaultDepthBegin = 6;
    static const std::size_t defaultStep = 8;
    static const std::size_t defaultChunkBytes = 1 << 20;   // 1 MB.

    static Id climb(const Id& id, Dir dir)
    {
        return (id << 3) + 1 + toIntegral(dir);
    }

private:
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

    void awaken(const Id& id);

    const BBox& m_bbox;
    const std::size_t m_depthBegin;
    std::size_t m_step;

    Node m_root;
    Node::NodeMap m_edges;
    Node::NodeSet m_anchors;
    Node::NodeSet m_awoken;

    std::unique_ptr<arbiter::Endpoint> m_endpoint;
};

class HierarchyClimber
{
public:
    HierarchyClimber(Hierarchy& hierarchy, std::size_t dimensions)
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
        m_node = &m_node->next(dir);
    }

    void count() { m_node->increment(); }
    std::size_t depthBegin() const { return m_depthBegin; }

private:
    Hierarchy& m_hierarchy;
    BBox m_bbox;

    const std::size_t m_depthBegin;

    std::size_t m_depth;
    std::size_t m_step;

    Node* m_node;
};

} // namespace entwine

