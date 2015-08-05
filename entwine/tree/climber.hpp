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

#include <cassert>
#include <cstddef>
#include <iomanip>
#include <iostream>

#include <entwine/types/bbox.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Point;

// Maintains the state of the current point as it traverses the virtual tree.
class Climber
{
public:
    Climber(const BBox& bbox, const Structure& structure);

    void magnify(const Point& point);
    std::size_t index() const { return m_index; }
    std::size_t depth() const { return m_depth; }
    std::size_t chunkId() const
    {
        return m_depth >= m_structure.coldDepthBegin() ? m_chunkId : 0;
    }

    const BBox& bbox() const { return m_bbox; }

    void goSwd() { climb(Dir::swd); m_bbox.goSwd(); }
    void goSed() { climb(Dir::sed); m_bbox.goSed(); }
    void goNwd() { climb(Dir::nwd); m_bbox.goNwd(); }
    void goNed() { climb(Dir::ned); m_bbox.goNed(); }
    void goSwu() { climb(Dir::swu); m_bbox.goSwu(); }
    void goSeu() { climb(Dir::seu); m_bbox.goSeu(); }
    void goNwu() { climb(Dir::nwu); m_bbox.goNwu(); }
    void goNeu() { climb(Dir::neu); m_bbox.goNeu(); }

    Climber getSwd() const { Climber c(*this); c.goSwd(); return c; }
    Climber getSed() const { Climber c(*this); c.goSed(); return c; }
    Climber getNwd() const { Climber c(*this); c.goNwd(); return c; }
    Climber getNed() const { Climber c(*this); c.goNed(); return c; }
    Climber getSwu() const { Climber c(*this); c.goSwu(); return c; }
    Climber getSeu() const { Climber c(*this); c.goSeu(); return c; }
    Climber getNwu() const { Climber c(*this); c.goNwu(); return c; }
    Climber getNeu() const { Climber c(*this); c.goNeu(); return c; }

    enum Dir
    {
        swd = 0,
        sed = 1,
        nwd = 2,
        ned = 3,
        swu = 4,
        seu = 5,
        nwu = 6,
        neu = 7
    };

private:
    const Structure& m_structure;
    std::size_t m_dimensions;

    std::size_t m_index;
    std::size_t m_depth;

    std::size_t m_levelIndex;
    std::size_t m_baseChunkPoints;
    std::size_t m_chunkId;
    std::size_t m_sparseDepthBegin;

    BBox m_bbox;

    void climb(Dir dir);
};

class ChunkClimber
{
public:
    ChunkClimber(const BBox& bbox, const Structure& structure)
        : m_dimensions(structure.dimensions())
        , m_depth(structure.nominalChunkDepth())
        , m_chunkId(structure.nominalChunkIndex())
        , m_baseChunkPoints(structure.baseChunkPoints())
        , m_coldDepthBegin(structure.coldDepthBegin())
        , m_bbox(bbox)
    { }

    std::size_t depth() const { return m_depth; }
    const BBox& bbox() const { return m_bbox; }

    std::size_t chunkId() const
    {
        return m_depth >= m_coldDepthBegin ? m_chunkId : 0;
    }

    ChunkClimber shimmy() const
    {
        ChunkClimber c(*this);
        c.climb(static_cast<Climber::Dir>(0));
        return c;
    }

    void goSwd() { climb(Climber::Dir::swd); m_bbox.goSwd(); }
    void goSed() { climb(Climber::Dir::sed); m_bbox.goSed(); }
    void goNwd() { climb(Climber::Dir::nwd); m_bbox.goNwd(); }
    void goNed() { climb(Climber::Dir::ned); m_bbox.goNed(); }
    void goSwu() { climb(Climber::Dir::swu); m_bbox.goSwu(); }
    void goSeu() { climb(Climber::Dir::seu); m_bbox.goSeu(); }
    void goNwu() { climb(Climber::Dir::nwu); m_bbox.goNwu(); }
    void goNeu() { climb(Climber::Dir::neu); m_bbox.goNeu(); }

    ChunkClimber getSwd() const { ChunkClimber c(*this); c.goSwd(); return c; }
    ChunkClimber getSed() const { ChunkClimber c(*this); c.goSed(); return c; }
    ChunkClimber getNwd() const { ChunkClimber c(*this); c.goNwd(); return c; }
    ChunkClimber getNed() const { ChunkClimber c(*this); c.goNed(); return c; }
    ChunkClimber getSwu() const { ChunkClimber c(*this); c.goSwu(); return c; }
    ChunkClimber getSeu() const { ChunkClimber c(*this); c.goSeu(); return c; }
    ChunkClimber getNwu() const { ChunkClimber c(*this); c.goNwu(); return c; }
    ChunkClimber getNeu() const { ChunkClimber c(*this); c.goNeu(); return c; }

private:
    std::size_t m_dimensions;

    std::size_t m_depth;
    std::size_t m_chunkId;

    std::size_t m_baseChunkPoints;
    std::size_t m_coldDepthBegin;

    BBox m_bbox;

    void climb(Climber::Dir dir)
    {
        ++m_depth;
        m_chunkId = (m_chunkId << m_dimensions) + 1 + dir * m_baseChunkPoints;
    }
};

class SplitClimber
{
public:
    SplitClimber(
            const Structure& structure,
            const BBox& bbox,
            const BBox& queryBBox,
            std::size_t depthBegin,
            std::size_t depthEnd)
        : m_structure(structure)
        , m_bbox(bbox)
        , m_queryBBox(queryBBox)
        , m_depthBegin(depthBegin)
        , m_depthEnd(std::min(depthEnd, structure.baseDepthEnd()))
        , m_index(0)
        , m_splits(1)
        , m_traversal()
        , m_xPos(0)
        , m_yPos(0)
        , m_zPos(0)
    { }

    bool next(bool terminate = false)
    {
        if (terminate || (m_depthEnd && depth() + 1 == m_depthEnd))
        {
            while (
                    depth() &&
                    static_cast<unsigned>(++m_traversal.back()) ==
                        m_structure.factor())
            {
                m_traversal.pop_back();
                m_splits /= 2;

                m_index = (m_index >> m_structure.dimensions()) - 1;

                m_xPos /= 2;
                m_yPos /= 2;
                m_zPos /= 2;
            }

            if (depth())
            {
                const auto current(m_traversal.back());
                ++m_index;

                if (current % 2)
                {
                    // Odd numbers: W->E.
                    ++m_xPos;
                }
                if (current == 2 || current == 6)
                {
                    // 2 or 6: E->W, N->S.
                    --m_xPos;
                    ++m_yPos;
                }
                else if (current == 4)
                {
                    // 4: E->W, S->N, D->U.
                    --m_xPos;
                    --m_yPos;
                    ++m_zPos;
                }
            }
        }
        else
        {
            m_traversal.push_back(0);
            m_splits *= 2;

            m_index = (m_index << m_structure.dimensions()) + 1;

            m_xPos *= 2;
            m_yPos *= 2;
            m_zPos *= 2;
        }

        if (depth())
        {
            if (depth() < m_depthBegin) return next();
            else if (overlaps()) return true;
            else return next(true);
        }
        else
        {
            return false;
        }
    }

    std::size_t index() const
    {
        return m_index;
    }

    bool overlaps() const
    {
        const Point& qMid(m_queryBBox.mid());

        return
            std::abs(qMid.x - midX()) <
                m_queryBBox.width() / 2.0 +
                m_bbox.width() / 2.0 / static_cast<double>(m_splits) &&

            std::abs(qMid.y - midY()) <
                m_queryBBox.depth() / 2.0 +
                m_bbox.depth() / 2.0 / static_cast<double>(m_splits) &&

            (
                !m_bbox.is3d() ||
                std::abs(qMid.z - midZ()) <
                    m_queryBBox.height() / 2.0 +
                    m_bbox.height() / 2.0 / static_cast<double>(m_splits));
    }

private:
    std::size_t depth() const
    {
        return m_traversal.size();
    }

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
    const BBox& m_bbox;

    // Query.
    const BBox& m_queryBBox;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;

    // State.
    std::size_t m_index;
    std::size_t m_splits;

    std::deque<int> m_traversal;

    std::size_t m_xPos;
    std::size_t m_yPos;
    std::size_t m_zPos;
};

} // namespace entwine

