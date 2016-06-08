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

#include <entwine/types/bbox.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Structure;

class SplitClimber
{
public:
    SplitClimber(
            const Structure& structure,
            const BBox& bbox,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            bool chunked = false)
        : m_structure(structure)
        , m_dimensions(m_structure.dimensions())
        , m_factor(m_structure.factor())
        , m_is3d(m_structure.is3d())
        , m_bbox(bbox)
        , m_qbox(qbox)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd)
        , m_chunked(chunked)
        , m_startDepth(m_chunked ? m_structure.nominalChunkDepth() : 0)
        , m_step(m_chunked ? m_structure.basePointsPerChunk() : 1)
        , m_index(m_chunked ? m_structure.nominalChunkIndex() : 0)
        , m_splits(1)
        , m_traversal()
        , m_xPos(0)
        , m_yPos(0)
        , m_zPos(0)
    {
        if (m_structure.baseDepthBegin()) next();
    }

    bool next(bool terminate = false);

    const Id& index() const
    {
        return m_index;
    }

    bool overlaps() const
    {
        const Point& qMid(m_qbox.mid());
        const double dblSplits(static_cast<double>(m_splits));

        return
            std::abs(qMid.x - midX()) <
                m_qbox.width() / 2.0 + m_bbox.width() / 2.0 / dblSplits &&
            std::abs(qMid.y - midY()) <
                m_qbox.depth() / 2.0 + m_bbox.depth() / 2.0 / dblSplits &&
            (
                !m_is3d ||
                std::abs(qMid.z - midZ()) <
                    m_qbox.height() / 2.0 + m_bbox.height() / 2.0 / dblSplits);
    }

    std::size_t depth() const
    {
        return m_startDepth + m_traversal.size();
    }

private:
    double midX() const
    {
        const double step(m_bbox.width() / static_cast<double>(m_splits));
        return m_bbox.min().x + m_xPos * step + step / 2.0;
    }

    double midY() const
    {
        const double step(m_bbox.depth() / static_cast<double>(m_splits));
        return m_bbox.min().y + m_yPos * step + step / 2.0;
    }

    double midZ() const
    {
        const double step(m_bbox.height() / static_cast<double>(m_splits));
        return m_bbox.min().z + m_zPos * step + step / 2.0;
    }

    // Tree.
    const Structure& m_structure;
    const std::size_t m_dimensions;
    const std::size_t m_factor;
    const bool m_is3d;
    const BBox& m_bbox;

    // Query.
    const BBox& m_qbox;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;

    // State.
    const bool m_chunked;
    const std::size_t m_startDepth;
    const std::size_t m_step;
    Id m_index;
    std::size_t m_splits;

    std::deque<unsigned int> m_traversal;

    std::size_t m_xPos;
    std::size_t m_yPos;
    std::size_t m_zPos;
};

} // namespace entwine

