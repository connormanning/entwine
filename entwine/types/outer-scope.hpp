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

    template<class... Args>
    std::shared_ptr<arbiter::Arbiter> getArbiter(Args&&... args) const
    {
        if (m_arbiter)
        {
            return m_arbiter;
        }
        else
        {
            return std::make_shared<arbiter::Arbiter>(
                    std::forward<Args>(args)...);
        }
    }

    template<class... Args>
    std::shared_ptr<PointPool> getPointPool(Args&&... args) const
    {
        if (m_pointPool)
        {
            return m_pointPool;
        }
        else
        {
            return std::make_shared<PointPool>(std::forward<Args>(args)...);
        }
    }

private:
    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::shared_ptr<PointPool> m_pointPool;
};

} // namespace entwine

