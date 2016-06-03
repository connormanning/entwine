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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/point-info.hpp>

namespace entwine
{

class OuterScope
{
public:
    void setArbiter(std::shared_ptr<arbiter::Arbiter> arbiter)
    {
        m_arbiter = arbiter;
    }

    void setPointPool(std::shared_ptr<PointPool> pointPool)
    {
        m_pointPool = pointPool;
    }

    void setNodePool(std::shared_ptr<Node::NodePool> nodePool)
    {
        m_nodePool = nodePool;
    }

    arbiter::Arbiter* getArbiterPtr() const { return m_arbiter.get(); }
    PointPool* getPointPoolPtr() const { return m_pointPool.get(); }
    Node::NodePool* getNodePoolPtr() const { return m_nodePool.get(); }

    template<class... Args>
    std::shared_ptr<arbiter::Arbiter> getArbiter(Args&&... args)
    {
        if (!m_arbiter)
        {
            m_arbiter = std::make_shared<arbiter::Arbiter>(
                    std::forward<Args>(args)...);
        }

        return m_arbiter;
    }

    template<class... Args>
    std::shared_ptr<PointPool> getPointPool(Args&&... args)
    {
        if (!m_pointPool)
        {
            return std::make_shared<PointPool>(std::forward<Args>(args)...);
        }

        return m_pointPool;
    }

    template<class... Args>
    std::shared_ptr<Node::NodePool> getNodePool(Args&&... args)
    {
        if (!m_nodePool)
        {
            return std::make_shared<Node::NodePool>(
                    std::forward<Args>(args)...);
        }

        return m_nodePool;
    }

private:
    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::shared_ptr<PointPool> m_pointPool;
    std::shared_ptr<Node::NodePool> m_nodePool;
};

} // namespace entwine

