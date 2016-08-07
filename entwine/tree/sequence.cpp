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

#include <algorithm>
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
    , m_manifest(m_metadata.manifest())
    , m_executor(builder.executor())
    , m_mutex(builder.mutex())
    , m_origin(m_manifest.split() ? m_manifest.split()->begin() : 0)
    , m_end(m_manifest.split() ? m_manifest.split()->end() : m_manifest.size())
    , m_added(0)
    , m_overlaps()
{
    const Bounds activeBounds(
            m_metadata.subset() ?
                *m_metadata.boundsSubset() : m_metadata.bounds());

    for (Origin i(m_origin); i < m_end; ++i)
    {
        const FileInfo& f(m_manifest.get(i));

        if (const Bounds* b = f.bounds())
        {
            if (activeBounds.overlaps(*b)) m_overlaps.push_back(i);
        }
        else
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
    FileInfo& info(m_manifest.get(origin));

    if (info.status() != FileInfo::Status::Outstanding)
    {
        return false;
    }
    else if (!m_executor.good(info.path()))
    {
        m_manifest.set(m_origin, FileInfo::Status::Omitted);
        return false;
    }
    else if (const Bounds* bounds = info.bounds())
    {
        if (!checkBounds(m_origin, *bounds, info.numPoints()))
        {
            m_manifest.set(m_origin, FileInfo::Status::Inserted);
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
    if (!m_metadata.bounds().overlaps(bounds))
    {
        const Subset* subset(m_metadata.subset());
        const bool primary(!subset || subset->primary());
        m_manifest.addOutOfBounds(origin, numPoints, primary);
        return false;
    }
    else if (const Bounds* boundsSubset = m_metadata.boundsSubset())
    {
        if (!boundsSubset->overlaps(bounds)) return false;
    }

    return true;
}

std::unique_ptr<Manifest::Split> Sequence::takeWork()
{
    std::unique_ptr<Manifest::Split> split;

    auto l(getLock());

    Manifest& manifest(m_metadata.manifest());

    auto b(m_overlaps.begin());
    const auto e(m_overlaps.end());

    auto pos(std::find_if(b, e, [this](Origin o) { return o >= m_origin; }));
    const auto end(std::find_if(b, e, [this](Origin o) { return o >= m_end; }));

    const float remaining(static_cast<float>(std::distance(pos, end)));
    const float ratioRemaining(remaining / static_cast<float>(manifest.size()));

    const std::size_t minTask(6);

    if (remaining >= 2 * minTask && ratioRemaining > 0.0025)
    {
        const float keepRatio(
                manifest.nominal() ?
                    heuristics::nominalKeepWorkRatio :
                    heuristics::defaultKeepWorkRatio);

        const std::size_t give(
                std::max<std::size_t>(
                    std::floor(remaining * (1 - keepRatio)),
                    minTask));

        const std::size_t keep(remaining - give);

        if (give >= minTask)
        {
            std::advance(pos, keep);
            m_end = *pos;
            split = manifest.split(m_end);
            std::cout << "Setting end at " << m_end << std::endl;
        }
        else std::cout << "Rejected - give: " << give << std::endl;
    }
    else std::cout << "Rejected - remaining: " << remaining << std::endl;

    return split;
}

} // namespace entwine

