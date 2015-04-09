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

#include <entwine/tree/branch.hpp>

namespace entwine
{

class FlatBranch : public Branch
{
public:
    FlatBranch(
            const Schema& schema,
            std::size_t dimensions,
            std::size_t depthBegin,
            std::size_t depthEnd);
    FlatBranch(
            const std::string& path,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    ~FlatBranch();

    virtual bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual bool hasPoint(std::size_t index);
    virtual Point getPoint(std::size_t index);
    virtual std::vector<char> getPointData(std::size_t index);

private:
    virtual void saveImpl(const std::string& path, Json::Value& meta);

    virtual void finalizeImpl(
            S3& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize);
};

} // namespace entwine

