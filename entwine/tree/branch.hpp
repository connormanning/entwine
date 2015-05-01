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
class Point;
class PointInfo;
class Pool;
class Roller;
class Schema;

class Branch
{
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
    bool accepts(Clipper* clipper, std::size_t index);

    // Returns true if this point was successfully stored.
    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller) = 0;

    // Returns the bytes of a point if there is a point at this index,
    // otherwise returns an empty vector - therefore the resultant vector is
    // guaranteed to be either schema.pointSize() or zero.
    //
    // May throw if a dimension cannot be read in the type/size specified
    // in the schema request.
    //
    // If a requested dimension in the schema does not exist in the tree's
    // schema, that dimension will be zeroed in the output.  A user is
    // responsible for querying the tree's schema in advance and requesting
    // only subsets of the existing schema.
    //
    //
    //
    // TODO Since we are being queried, we are assuming that addPoint() is no
    // longer be called so the data is now constant.  Does this call need to be
    // supported during the build phase?
    //
    // If so we need to lock here.  If not, then it should be documented and
    // enforced that this cannot be called while points are being inserted.
    //
    // The same is true for hasPoint().
    virtual std::vector<char> getPointData(std::size_t index) = 0;

    // Returns true if there is a point at this index.
    virtual bool hasPoint(std::size_t index);

    // Returns the point at this index.  If no point exists here, then returns
    // a point with X and Y set to INFINITY.
    virtual Point getPoint(std::size_t index) = 0;

    virtual void grow(Clipper* clipper, std::size_t index) { }
    virtual void clip(Clipper* clipper, std::size_t index) { }

    // Writes necessary metadata and point data to disk.
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
    virtual void saveImpl(Json::Value& meta) = 0;
    virtual void finalizeImpl(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize) = 0;

    const Schema& schema()      const;
    std::size_t depthBegin()    const;
    std::size_t depthEnd()      const;
    std::size_t indexBegin()    const;
    std::size_t indexEnd()      const;
    std::size_t size()          const;
    std::size_t dimensions()    const;

    Source& m_source;

private:
    const Schema& m_schema;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const std::size_t m_indexBegin;
    const std::size_t m_indexEnd;

    const std::size_t m_dimensions;
};

} // namespace entwine

