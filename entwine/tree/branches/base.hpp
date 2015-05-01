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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <entwine/tree/branch.hpp>
#include <entwine/types/elastic-atomic.hpp>

namespace Json
{
    class Value;
}

namespace pdal
{
    class PointView;
}

namespace entwine
{

class Schema;
class SimplePointTable;

class BaseBranch : public Branch
{
public:
    BaseBranch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthEnd);
    BaseBranch(
            Source& source,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    ~BaseBranch();

    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual bool hasPoint(const std::size_t index);
    virtual Point getPoint(std::size_t index);
    virtual std::vector<char> getPointData(std::size_t index);

private:
    virtual void finalizeImpl(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize);

    virtual void saveImpl(Json::Value& meta);
    void load(const Json::Value& meta);

    char* getLocation(std::size_t index);

    std::vector<ElasticAtomic<const Point*>> m_points;
    std::vector<char> m_data;
    std::vector<std::mutex> m_locks;
};

} // namespace entwine

