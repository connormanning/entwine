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
#include <map>
#include <string>

#include <entwine/third/json/json.hpp>
#include <entwine/tree/climber.hpp>

namespace entwine
{

const std::size_t step(6);
const std::string countKey("n");

class Node
{
public:
    Node() : m_count(0), m_children() { }
    Node(const Json::Value& json)
        : m_count(json[countKey].asUInt64())
        , m_children()
    {
        if (!m_count) throw std::runtime_error("Invalid hierarchy count");

        for (const auto& key : json.getMemberNames())
        {
            if (key != countKey)
            {
                m_children[Climber::stringToDir(key)] = Node(json[key]);
            }
        }
    }

    Node& next(const Climber::Dir dir) { return m_children[dir]; }
    void increment() { ++m_count; }
    std::size_t count() const { return m_count; }

    void merge(Node& other)
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

    void insertInto(Json::Value& json) const
    {
        if (m_count)
        {
            json[countKey] = static_cast<Json::UInt64>(m_count);
            for (const auto& c : m_children)
            {
                c.second.insertInto(json[Climber::dirToString(c.first)]);
            }
        }
    }

    const std::map<Climber::Dir, Node>& children() const { return m_children; }

private:
    std::map<Climber::Dir, Node>& children() { return m_children; }

    std::size_t m_count;
    std::map<Climber::Dir, Node> m_children;
};

class Hierarchy
{
public:
    Hierarchy() : m_root() { }
    Hierarchy(const Json::Value& json) : m_root(json) { }

    Node& root() { return m_root; }

    Json::Value toJson() const
    {
        Json::Value json;
        m_root.insertInto(json);
        return json;
    }

    void merge(Hierarchy& other)
    {
        m_root.merge(other.root());
    }

private:
    Node m_root;
};

} // namespace entwine

