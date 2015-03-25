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
    virtual const Point* getPoint(std::size_t index);
    virtual std::vector<char> getPointData(
            std::size_t index,
            const Schema& schema);

private:
    virtual void saveImpl(const std::string& path, Json::Value& meta);
};

} // namespace entwine

