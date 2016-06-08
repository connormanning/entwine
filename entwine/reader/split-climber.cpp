/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/split-climber.hpp>

namespace entwine
{

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

