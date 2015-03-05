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

#include <mutex>
#include <vector>

#include "tree/branch.hpp"
#include "types/elastic-atomic.hpp"

namespace entwine
{

class Schema;

class BaseBranch : public Branch
{
public:
    BaseBranch(
            const Schema& schema,
            std::size_t begin,
            std::size_t end);
    BaseBranch(
            const Schema& schema,
            std::size_t begin,
            std::size_t end,
            std::vector<char>* data);
    ~BaseBranch();

    virtual bool putPoint(PointInfo** toAddPtr, const Roller& roller);
    virtual const Point* getPoint(std::size_t index) const;

    virtual void save(const std::string& dir, Json::Value& meta) const;
    virtual void load(const std::string& dir, const Json::Value& meta);

private:
    std::vector<ElasticAtomic<const Point*>> m_points;
    std::unique_ptr<std::vector<char>> m_data;
    std::vector<std::mutex> m_locks;
};

} // namespace entwine

