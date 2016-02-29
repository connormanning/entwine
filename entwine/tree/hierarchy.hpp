/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cstddef>
#include <deque>
#include <map>
#include <string>

#include <entwine/third/json/json.hpp>
#include <entwine/types/bbox.hpp>

namespace entwine
{

class Node
{
public:
    Node() : m_count(0), m_children() { }
    Node(const Json::Value& json);

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

    typedef std::map<Dir, Node> Children;
    const Children& children() const { return m_children; }

private:
    Children& children() { return m_children; }

    std::size_t m_count;
    Children m_children;
};

class Hierarchy
{
public:
    Hierarchy(const BBox& bbox, std::size_t depth)
        : m_bbox(bbox)
        , m_depthBegin(depth)
        , m_root()
    { }

    Hierarchy(const BBox& bbox, Json::Value& json)
        : m_bbox(bbox)
        , m_depthBegin(json.removeMember("depthBegin").asUInt64())
        , m_root(json)
    {
        m_root = Node(json);
    }

    Node& root() { return m_root; }

    Json::Value toJson() const
    {
        Json::Value json;
        json["depthBegin"] = static_cast<Json::UInt64>(m_depthBegin);
        m_root.insertInto(json);
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

private:
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

    Node m_root;
};

} // namespace entwine

