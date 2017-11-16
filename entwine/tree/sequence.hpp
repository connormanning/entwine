/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <iostream>
#include <memory>

#include <entwine/types/defs.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/tree/builder.hpp>

namespace entwine
{

class Builder;
class Executor;
class Metadata;

class Sequence
{
    friend class Builder;

public:
    Sequence(Builder& builder);

    std::unique_ptr<Origin> next(std::size_t max);
    bool done() const { auto l(getLock()); return m_origin < m_end; }

    // Stop this build as soon as possible.  All partially inserted paths will
    // be completed, and non-inserted paths can be added by continuing this
    // build later.
    void stop()
    {
        auto l(getLock());
        m_end = std::min(m_end, m_origin + 1);
        std::cout << "Stopping - setting end at " << m_end << std::endl;
    }

private:
    std::unique_lock<std::mutex> getLock() const
    {
        return std::unique_lock<std::mutex>(m_mutex);
    }

    bool checkInfo(Origin origin);

    bool checkBounds(
            Origin origin,
            const Bounds& bounds,
            std::size_t numPoints);

    Metadata& m_metadata;
    Manifest* m_manifest;
    std::mutex& m_mutex;

    Origin m_origin;
    Origin m_end;
    std::size_t m_added;

    std::vector<Origin> m_overlaps;
};

} // namespace entwine

