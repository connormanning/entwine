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
#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class ReffedSelfChunk;

class SelfChunk
{
public:
    SelfChunk(const ReffedSelfChunk& ref)
        : m_ref(ref)
    { }

    virtual ~SelfChunk() { }

    virtual bool insert(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper* clipper) = 0;

    virtual Cells acquire(PointPool& pointPool) = 0;

protected:
    const ReffedSelfChunk& m_ref;
};

class ReffedSelfChunk
{
public:
    ReffedSelfChunk(
            const ChunkKey& key,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            Hierarchy& hierarchy)
        : m_key(key)
        , m_metadata(m_key.metadata())
        , m_out(out)
        , m_tmp(tmp)
        , m_pointPool(pointPool)
        , m_hierarchy(hierarchy)
    {
        if (m_key.d == 0) m_key.d = m_metadata.structure().body();
    }

    virtual bool insert(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper& clipper)
    {
        if (clipper.insert(*this)) ref(climber);
        return m_chunk->insert(cell, climber, &clipper);
    }

    void ref(const NewClimber& climber);
    void unref(Origin o);

    SelfChunk& chunk() { return *m_chunk; }
    /*
    std::size_t np() const { return m_np; }
    void setNp(uint64_t np) { m_np = np; }
    */

    const ChunkKey& key() const { return m_key; }
    const Metadata& metadata() const { return m_metadata; }
    const arbiter::Endpoint& out() const { return m_out; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }
    PointPool& pointPool() const { return m_pointPool; }
    Hierarchy& hierarchy() const { return m_hierarchy; }

private:
    ChunkKey m_key;
    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;
    Hierarchy& m_hierarchy;

    std::mutex m_mutex;
    // std::size_t m_np = 0;
    std::unique_ptr<SelfChunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

class SelfContiguousChunk : public SelfChunk
{
public:
    SelfContiguousChunk(const ReffedSelfChunk& c)
        : SelfChunk(c)
        , m_pointsAcross(1UL << c.metadata().structure().body())
        , m_tubes(m_pointsAcross * m_pointsAcross)
    {
        assert(c.key().depth() < c.metadata().structure().tail());

        for (std::size_t d(0); d < dirEnd(); ++d)
        {
            m_children.emplace_back(
                    makeUnique<ReffedSelfChunk>(
                        c.key().getStep(toDir(d)),
                        c.out(),
                        c.tmp(),
                        c.pointPool(),
                        c.hierarchy()));
        }
    }

    virtual bool insert(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper* clipper) override
    {
        const Xyz& pk(climber.pointKey().position());
        const std::size_t i(
                (pk.y % m_pointsAcross) * m_pointsAcross +
                (pk.x % m_pointsAcross));

        assert(i < m_tubes.size());

        Tube::Insertion attempt(m_tubes[i].insert(climber, cell));

        if (attempt.done()) return true;
        else if (!clipper) return false;

        const Dir dir(getDirection(m_ref.key().bounds().mid(), cell->point()));
        auto& rc(*m_children.at(toIntegral(dir)));

        climber.step(cell->point());

        return rc.insert(cell, climber, *clipper);
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

    std::vector<std::unique_ptr<ReffedSelfChunk>> m_children;
};

class SelfMappedChunk : public SelfChunk
{
public:
    SelfMappedChunk(const ReffedSelfChunk& c)
        : SelfChunk(c)
        , m_child(
                c.key().getStep(),
                c.out(),
                c.tmp(),
                c.pointPool(),
                c.hierarchy())
    { }

    virtual bool insert(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper* clipper) override
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const Xyz& pk(climber.pointKey().position());
            Tube::Insertion attempt(m_tubes[pk.y][pk.x].insert(climber, cell));

            if (attempt.done()) return true;
        }

        climber.step(cell->point());
        return m_child.insert(cell, climber, *clipper);
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

    ReffedSelfChunk m_child;
};

} // namespace entwine

