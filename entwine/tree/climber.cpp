/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/climber.hpp>

#include <entwine/types/point.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/tree/hierarchy.hpp>

namespace entwine
{

Climber::Climber(
        const BBox& bbox,
        const Structure& structure,
        Hierarchy* hierarchy)
    : m_structure(structure)
    , m_dimensions(structure.dimensions())
    , m_factor(structure.factor())
    , m_is3d(structure.is3d())
    , m_tubular(structure.tubular())
    , m_sparseDepthBegin(
            structure.dynamicChunks() ? structure.sparseDepthBegin() : 0)
    , m_index(0)
    , m_chunkId(structure.nominalChunkIndex())
    , m_tick(0)
    , m_depth(0)
    , m_depthChunks(1)
    , m_chunkNum(0)
    , m_pointsPerChunk(structure.basePointsPerChunk())
    , m_bboxOriginal(bbox)
    , m_bbox(bbox)
    , m_bboxChunk(bbox)
    , m_hierarchyClimber(
            hierarchy ?
                new HierarchyClimber(
                    *hierarchy,
                    structure.tubular() || m_dimensions == 3 ? 3 : 2) :
                nullptr)
{ }

Climber::Climber(const Climber& other)
    : m_structure(other.m_structure)
    , m_dimensions(other.m_dimensions)
    , m_factor(other.m_factor)
    , m_is3d(other.m_is3d)
    , m_tubular(other.m_tubular)
    , m_sparseDepthBegin(other.m_sparseDepthBegin)
    , m_index(other.m_index)
    , m_chunkId(other.m_chunkId)
    , m_tick(other.m_tick)
    , m_depth(other.m_depth)
    , m_depthChunks(other.m_depthChunks)
    , m_chunkNum(other.m_chunkNum)
    , m_pointsPerChunk(other.m_pointsPerChunk)
    , m_bboxOriginal(other.m_bboxOriginal)
    , m_bbox(other.m_bbox)
    , m_bboxChunk(other.m_bboxChunk)
    , m_hierarchyClimber(
            other.m_hierarchyClimber ?
                new HierarchyClimber(*other.m_hierarchyClimber) : nullptr)
{ }

Climber& Climber::operator=(const Climber& other)
{
    m_index = other.m_index;
    m_chunkId = other.m_chunkId;
    m_tick = other.m_tick;
    m_depth = other.m_depth;
    m_depthChunks = other.m_depthChunks;
    m_chunkNum = other.m_chunkNum;
    m_pointsPerChunk = other.m_pointsPerChunk;
    m_bbox = other.m_bbox;
    m_bboxChunk = other.m_bboxChunk;

    if (other.m_hierarchyClimber)
    {
        m_hierarchyClimber.reset(
                new HierarchyClimber(*other.m_hierarchyClimber));
    }

    return *this;
}

Climber::~Climber() { }

void Climber::reset()
{
    m_index = 0;
    m_chunkId = m_structure.nominalChunkIndex();
    m_tick = 0;
    m_depth = 0;
    m_depthChunks = 1;
    m_chunkNum = 0;
    m_pointsPerChunk = m_structure.basePointsPerChunk();
    m_bbox = m_bboxOriginal;
    m_bboxChunk = m_bboxOriginal;
    if (m_hierarchyClimber) m_hierarchyClimber->reset();
}

void Climber::magnify(const Point& point)
{
    const Point& mid(m_bbox.mid());

    if (m_tubular && m_depth < Tube::maxTickDepth())
    {
        m_tick <<= 1;
        if (point.z >= mid.z) ++m_tick;
    }

    switch (getDirection(point, mid))
    {
        case Dir::swd: goSwd(); break;
        case Dir::sed: goSed(); break;
        case Dir::nwd: goNwd(); break;
        case Dir::ned: goNed(); break;
        case Dir::swu: goSwu(); break;
        case Dir::seu: goSeu(); break;
        case Dir::nwu: goNwu(); break;
        case Dir::neu: goNeu(); break;
    }

    if (m_hierarchyClimber && m_depth > m_hierarchyClimber->depthBegin())
    {
        m_hierarchyClimber->magnify(point);
    }
}

void Climber::magnifyTo(const Point& point, const std::size_t depth)
{
    while (m_depth < depth) magnify(point);
}

void Climber::magnifyTo(const BBox& raw)
{
    BBox bbox(BBox(raw.min(), raw.max(), m_is3d).growBy(.01));
    while (!bbox.contains(m_bbox, true)) magnify(bbox.mid());
}

void Climber::climb(const Dir dir)
{
    if (++m_depth > m_structure.nominalChunkDepth())
    {
        if (m_depth <= m_sparseDepthBegin || !m_sparseDepthBegin)
        {
            const std::size_t chunkRatio(
                    (m_index - m_chunkId).getSimple() /
                    (m_pointsPerChunk.getSimple() / m_factor));

            assert(chunkRatio < m_factor);

            m_chunkId <<= m_dimensions;
            m_chunkId.incSimple();
            m_chunkId += chunkRatio * m_pointsPerChunk;

            assert(chunkRatio < m_factor);

            switch (static_cast<Dir>(chunkRatio))
            {
                case Dir::swd: m_bboxChunk.goSwd(m_tubular); break;
                case Dir::sed: m_bboxChunk.goSed(m_tubular); break;
                case Dir::nwd: m_bboxChunk.goNwd(m_tubular); break;
                case Dir::ned: m_bboxChunk.goNed(m_tubular); break;
                case Dir::swu: m_bboxChunk.goSwu(); break;
                case Dir::seu: m_bboxChunk.goSeu(); break;
                case Dir::nwu: m_bboxChunk.goNwu(); break;
                case Dir::neu: m_bboxChunk.goNeu(); break;
            }

            if (m_depth >= m_structure.coldDepthBegin())
            {
                m_chunkNum =
                    (m_chunkId - m_structure.coldIndexBegin()) /
                    m_pointsPerChunk;
            }

            m_depthChunks *= m_factor;
        }
        else
        {
            m_chunkNum += m_depthChunks;

            m_chunkId <<= m_dimensions;
            m_chunkId.incSimple();

            m_pointsPerChunk *= m_factor;
        }
    }

    m_index <<= m_dimensions;
    m_index.incSimple();

    // If this is a hybrid index, then we're tracking the BBox in 3d, but
    // climbing in 2d.  If so, normalize the direction to 2d.
    m_index += static_cast<std::size_t>(
            m_tubular ? toDir((toIntegral(dir) % 4)) : dir);
}

HierarchyClimber& Climber::hierarchyClimber()
{
    return *m_hierarchyClimber;
}

void Climber::count()
{
    if (m_depth >= m_hierarchyClimber->depthBegin())
    {
        m_hierarchyClimber->count();
    }
}

bool SplitClimber::next(bool terminate)
{
    bool redo(false);

    do
    {
        redo = false;

        if (terminate || (m_depthEnd && depth() + 1 >= m_depthEnd))
        {
            // Move shallower.
            while (
                    (m_traversal.size() && ++m_traversal.back() == m_factor) ||
                    (depth() > m_structure.sparseDepthBegin() + 1))
            {
                if (depth() <= m_structure.sparseDepthBegin() + 1)
                {
                    m_index -= (m_factor - 1) * m_step;
                }

                m_index >>= m_dimensions;

                m_traversal.pop_back();
                m_splits /= 2;

                m_xPos /= 2;
                m_yPos /= 2;
                if (m_is3d) m_zPos /= 2;
            }

            // Move laterally.
            if (m_traversal.size())
            {
                const auto current(m_traversal.back());
                m_index += m_step;

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
            // Move deeper.
            m_traversal.push_back(0);
            m_splits *= 2;

            m_index = (m_index << m_dimensions) + 1;

            m_xPos *= 2;
            m_yPos *= 2;
            if (m_is3d) m_zPos *= 2;
        }

        if (m_traversal.size())
        {
            if (
                    depth() < m_depthBegin ||
                    depth() < m_structure.baseDepthBegin() ||
                    (m_chunked && depth() < m_structure.coldDepthBegin()))
            {
                terminate = false;
                redo = true;
            }
            else if (overlaps())
            {
                return true;
            }
            else
            {
                terminate = true;
                redo = true;
            }
        }
        else
        {
            return false;
        }
    }
    while (redo);

    return false;
}

} // namespace entwine

