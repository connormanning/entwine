/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cassert>
#include <cstddef>
#include <mutex>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/new-climber.hpp>
#include <entwine/tree/new-clipper.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/types/tube.hpp>

namespace entwine
{

class NewChunk
{
public:
    NewChunk() { }
    virtual ~NewChunk() { }

    virtual Tube::Insertion insert(
            Cell::PooledNode& cell,
            const NewClimber& climber) = 0;

    virtual Cells acquire(PointPool& pointPool) = 0;
};

class NewContiguousChunk : public NewChunk
{
public:
    NewContiguousChunk(std::size_t pointsAcross)
        : NewChunk()
        , m_pointsAcross(pointsAcross)
        , m_tubes(m_pointsAcross * m_pointsAcross)
    { }

    virtual Tube::Insertion insert(
            Cell::PooledNode& cell,
            const NewClimber& climber) override
    {
        const Xyz& pk(climber.pointKey().position());
        const std::size_t i(
                (pk.y % m_pointsAcross) * m_pointsAcross +
                (pk.x % m_pointsAcross));

        assert(i < m_tubes.size());
        return m_tubes[i].insert(climber, cell);
    }

private:
    virtual Cells acquire(PointPool& pointPool) override
    {
        Cells cells(pointPool.cellPool());

        for (auto& tube : m_tubes)
        {
            for (auto& inner : tube) cells.push(std::move(inner.second));
        }

        m_tubes.clear();
        return cells;
    }

    const std::size_t m_pointsAcross;
    std::vector<Tube> m_tubes;
};

class NewMappedChunk : public NewChunk
{
public:
    NewMappedChunk(std::size_t pointsAcross)
        : NewChunk()
    { }

    virtual Tube::Insertion insert(
            Cell::PooledNode& cell,
            const NewClimber& climber) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const Xyz& pk(climber.pointKey().position());
        return m_tubes[pk.y][pk.x].insert(climber, cell);
    }

private:
    virtual Cells acquire(PointPool& pointPool) override
    {
        Cells cells(pointPool.cellPool());

        for (auto& y : m_tubes)
        {
            for (auto& x : y.second)
            {
                Tube& t(x.second);
                for (auto& inner : t) cells.push(std::move(inner.second));
            }
        }

        m_tubes.clear();
        return cells;
    }

    std::mutex m_mutex;
    std::map<uint64_t, std::map<uint64_t, Tube>> m_tubes;
};

} // namespace entwine

