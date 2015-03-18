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

namespace Json
{
    class Value;
}

namespace entwine
{

class Point;
class PointInfo;
class Roller;
class Schema;

class Branch
{
public:
    Branch(
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthBegin,
            std::size_t depthEnd);
    Branch(
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    virtual ~Branch();

    // Returns true if this branch is responsible for the specified index.
    // Does not necessarily mean that this index contains a valid Point.
    bool accepts(std::size_t index) const;

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
    virtual std::vector<char> getPointData(
            std::size_t index,
            const Schema& schema) = 0;

    // Returns the point coordinates at this index.  Null pointer indicates
    // that there is no point at this index.
    virtual const Point* getPoint(std::size_t index) = 0;

    // Writes necessary metadata and point data to disk.
    void save(const std::string& path, Json::Value& meta);

protected:
    virtual void saveImpl(const std::string& path, Json::Value& meta) = 0;

    const Schema& schema()      const;
    std::size_t depthBegin()    const;
    std::size_t depthEnd()      const;
    std::size_t indexBegin()    const;
    std::size_t indexEnd()      const;
    std::size_t size()          const;

private:
    const Schema& m_schema;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const std::size_t m_indexBegin;
    const std::size_t m_indexEnd;
};

} // namespace entwine

