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

    // Returns true if the specified index is owned by this branch.  Does not
    // necessarily mean that a valid Point resides at this index.
    bool accepts(std::size_t index) const;

    // Returns true if this point was successfully stored.
    virtual bool putPoint(PointInfo** toAddPtr, const Roller& roller) = 0;

    // Null pointer indicates that there is no point at this index.
    virtual const Point* getPoint(std::size_t index) const = 0;

    // Writes necessary metadata and point data to disk.
    void save(const std::string& path, Json::Value& meta) const;

protected:
    virtual void saveImpl(const std::string& path, Json::Value& meta) const = 0;

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

