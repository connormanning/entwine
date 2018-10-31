/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/sequence.hpp>

#include <iterator>

#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Sequence::Sequence(Metadata& metadata, std::mutex& mutex)
    : m_metadata(metadata)
    , m_files(metadata.mutableFiles())
    , m_mutex(mutex)
    , m_origin(0)
    , m_end(m_files.size())
    , m_added(0)
    , m_overlaps()
{
    const Bounds activeBounds(
            m_metadata.subset() ?
                m_metadata.subset()->bounds() :
                m_metadata.boundsConforming());

    for (Origin i(m_origin); i < m_end; ++i)
    {
        const FileInfo& f(m_files.get(i));
        const Bounds* b(f.boundsEpsilon());

        if (!b || activeBounds.overlaps(*b, true))
        {
            m_overlaps.push_back(i);
        }
    }

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
    FileInfo& info(m_files.get(origin));

    if (info.status() != FileInfo::Status::Outstanding)
    {
        return false;
    }
    else if (!Executor::get().good(info.path()))
    {
        m_files.set(origin, FileInfo::Status::Omitted);
        return false;
    }
    else if (const Bounds* infoBounds = info.boundsEpsilon())
    {
        if (!checkBounds(origin, *infoBounds, info.points()))
        {
            m_files.set(origin, FileInfo::Status::Inserted);
            return false;
        }
    }

    return true;
}

bool Sequence::checkBounds(
        const Origin origin,
        const Bounds& bounds,
        const std::size_t points)
{
    if (!m_metadata.boundsCubic().overlaps(bounds, true))
    {
        const Subset* subset(m_metadata.subset());
        const bool primary(!subset || subset->primary());
        m_files.addOutOfBounds(origin, points, primary);
        return false;
    }
    else if (const Subset* subset = m_metadata.subset())
    {
        if (!subset->bounds().overlaps(bounds, true)) return false;
    }

    return true;
}

} // namespace entwine

