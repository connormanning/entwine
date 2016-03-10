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
#include <string>

#include <entwine/third/json/json.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/bbox.hpp>

namespace entwine
{

class Node
{
public:
    Node() : m_count(0), m_children() { }
    Node(const char*& pos);

    Node& next(Dir dir) { return m_children[dir]; }

    const Node* maybeNext(Dir dir) const
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
    void insertInto(std::string& s) const;

    typedef std::map<Dir, Node> Children;
    const Children& children() const { return m_children; }

private:
    Children& children() { return m_children; }

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
    { }

    Hierarchy(
            const BBox& bbox,
            const Json::Value& json,
            const arbiter::Endpoint& ep)
        : m_bbox(bbox)
        , m_depthBegin(json["depthBegin"].asUInt64())
        , m_step(json["step"].asUInt64())
        , m_root()
    {
        const std::vector<char> bin(ep.getSubpathBinary("0"));
        const char* pos(bin.data());
        m_root = Node(pos);
    }

    Node& root() { return m_root; }

    Json::Value toJson(const arbiter::Endpoint& ep) const
    {
        Json::Value json;
        json["depthBegin"] = static_cast<Json::UInt64>(m_depthBegin);
        json["step"] = static_cast<Json::UInt64>(m_step);

        ep.putSubpath("0", toBinary());

        return json;
    }

    Json::Value query(
            BBox qbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    void merge(Hierarchy& other)
    {
        m_root.merge(other.root());
    }

    std::size_t depthBegin() const { return m_depthBegin; }
    std::size_t step() const { return m_step; }
    const BBox& bbox() const { return m_bbox; }

private:
    static const std::size_t defaultDepthBegin = 6;
    static const std::size_t defaultStep = 0;       // Denote flat structure.

    std::string toBinary() const
    {
        std::string s;
        m_root.insertInto(s);
        return s;
    }

    void traverse(
            Node& out,
            std::deque<Dir>& lag,
            const Node& cur,
            const BBox& cbox,
            const BBox& qbox,
            std::size_t depth,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    void accumulate(
            Node& node,
            std::deque<Dir>& lag,
            const Node& cur,
            std::size_t depth,
            std::size_t depthEnd) const;

    const BBox& m_bbox;
    const std::size_t m_depthBegin;
    const std::size_t m_step;

    Node m_root;
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

