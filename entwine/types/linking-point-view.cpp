/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/linking-point-view.hpp>

namespace entwine
{

LinkingPointView::LinkingPointView(SizedPointTable& table)
    : PointView(table)
{
    for (std::size_t i(0); i < table.size(); ++i)
    {
        m_index[i] = i;
    }
}

} // namespace entwine

