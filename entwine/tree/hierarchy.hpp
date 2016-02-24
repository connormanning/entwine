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

#include <entwine/tree/climber.hpp>

namespace entwine
{

const std::size_t step(6);

class Hierarchy
{
public:
    Hierarchy();

    HierarchyNode& root() { return m_root; }

private:
    class HierarchyNode
    {
    public:
        HierarchyNode() : m_count(0), m_children() { }

        HierarchyNode& next(const Climber::Dir dir)
        {
            return m_children[dir];
        }

    private:
        std::size_t m_count(0);
        std::map<Climber::Dir, HierarchyNode> m_children;
    };

    HierarchyNode m_root;

    // std::map<Id, HierarchyNode> m_nodes;
};

} // namespace entwine

