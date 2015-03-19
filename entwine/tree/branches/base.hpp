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
    class PointTable;
}

namespace entwine
{

class Schema;

class BaseBranch : public Branch
{
public:
    BaseBranch(
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthEnd);
    BaseBranch(
            const std::string& path,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    ~BaseBranch();

    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual const Point* getPoint(std::size_t index);
    virtual std::vector<char> getPointData(
            std::size_t index,
            const Schema& schema);

private:
    virtual void saveImpl(const std::string& path, Json::Value& meta);
    void load(const std::string& path, const Json::Value& meta);

    std::vector<ElasticAtomic<const Point*>> m_points;
    std::unique_ptr<pdal::PointTable> m_table;
    std::unique_ptr<pdal::PointView> m_data;
    std::vector<std::mutex> m_locks;
};

} // namespace entwine

