/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/sequence.hpp>

#include <iterator>

#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Sequence::Sequence(Builder& builder)
    : m_metadata(*builder.m_metadata)
    , m_manifest(m_metadata.manifestPtr())
    , m_mutex(builder.mutex())
    , m_origin(0)
    , m_end(m_manifest ? m_manifest->size() : 0)
    , m_added(0)
    , m_overlaps()
{
    if (!m_manifest) return;

    const Bounds activeBounds(
            // m_metadata.subset() ?
                // *m_metadata.boundsNativeSubset() :
                m_metadata.boundsNativeCubic());

    for (Origin i(m_origin); i < m_end; ++i)
    {
        const FileInfo& f(m_manifest->get(i));
        const Bounds* b(f.boundsEpsilon());

        if (!b || activeBounds.overlaps(*b, true))
        {
            m_overlaps.push_back(i);
        }
    }

    /*
    if (builder.verbose() && m_metadata.subset())
    {
        std::cout << "Overlaps: " << m_overlaps.size() << std::endl;
    }
    */

    m_origin = m_overlaps.empty() ? m_end : m_overlaps.front();
}

std::unique_ptr<Origin> Sequence::next(std::size_t max)
{
    auto lock(getLock());
    while (m_origin < m_end && (!max || m_added < max))
    {
        const Origin active(m_origin++);

        if (checkInfo(active))
        {
            ++m_added;
            return makeUnique<Origin>(active);
        }
    }

    return std::unique_ptr<Origin>();
}

bool Sequence::checkInfo(Origin origin)
{
    FileInfo& info(m_manifest->get(origin));

    if (info.status() != FileInfo::Status::Outstanding)
    {
        return false;
    }
    else if (!Executor::get().good(info.path()))
    {
        m_manifest->set(origin, FileInfo::Status::Omitted);
        return false;
    }
    else if (const Bounds* infoBounds = info.boundsEpsilon())
    {
        if (!checkBounds(origin, *infoBounds, info.numPoints()))
        {
            m_manifest->set(origin, FileInfo::Status::Inserted);
            return false;
        }
    }

    return true;
}

bool Sequence::checkBounds(
        const Origin origin,
        const Bounds& bounds,
        const std::size_t numPoints)
{
    /*
    if (!m_metadata.boundsNativeCubic().overlaps(bounds, true))
    {
        const Subset* subset(m_metadata.subset());
        const bool primary(!subset || subset->primary());
        m_manifest->addOutOfBounds(origin, numPoints, primary);
        return false;
    }
    else if (const auto boundsSubset = m_metadata.boundsNativeSubset())
    {
        if (!boundsSubset->overlaps(bounds)) return false;
    }
    */

    return true;
}

} // namespace entwine

