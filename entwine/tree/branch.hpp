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
#include <set>
#include <string>
#include <vector>

#include <entwine/drivers/source.hpp>

namespace Json
{
    class Value;
}

namespace entwine
{

class Clipper;
struct Entry;
class Point;
class PointInfo;
class Pool;
class Roller;
class Schema;

class Branch
{
friend class Registry;

public:
    Branch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthBegin,
            std::size_t depthEnd);
    Branch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    virtual ~Branch();

    // Returns true if this branch is responsible for the specified index.
    // Does not necessarily mean that this index contains a valid Point.
    //
    // When the supplied Clipper destructs, internal data for this index may
    // be freed.
    bool accepts(Clipper* clipper, std::size_t index);

    // Returns true if this point was successfully stored.
    bool addPoint(PointInfo** toAddPtr, const Roller& roller);

    // Writes necessary metadata and point data to the Source for this branch
    // so that the build may be queried in its current state, and the build may
    // be continued later.
    void save(Json::Value& meta);

    // Export all populated chunks of the completed tree.
    void finalize(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            const std::size_t start,
            const std::size_t chunkSize);

    static std::size_t calcOffset(
            std::size_t depth,
            std::size_t dimensions);

protected:
    virtual std::unique_ptr<Entry> getEntry(std::size_t index) = 0;
    virtual void saveImpl(Json::Value& meta) { }
    virtual void finalizeImpl(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize) = 0;

    // For branches that allocate on demand.
    virtual void grow(Clipper* clipper, std::size_t index) { }
    virtual void clip(Clipper* clipper, std::size_t index) { }

    const Schema& schema()      const;
    std::size_t depthBegin()    const;
    std::size_t depthEnd()      const;
    std::size_t indexBegin()    const;
    std::size_t indexEnd()      const;
    std::size_t indexSpan()     const;
    std::size_t dimensions()    const;

    Source& m_source;
    std::set<std::size_t> m_ids;

private:
    const Schema& m_schema;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const std::size_t m_indexBegin;
    const std::size_t m_indexEnd;

    const std::size_t m_dimensions;
};

} // namespace entwine

