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

class SelfChunk
{
    friend class ReffedSelfChunk;

public:
    SelfChunk(const ReffedSelfChunk& ref);
    virtual ~SelfChunk() { }

    virtual bool insert(Cell::PooledNode& cell, NewClimber& climber) = 0;
    virtual ReffedSelfChunk& step(const Cell::PooledNode& cell) = 0;

protected:
    virtual Cells acquire() = 0;
    virtual void init() { m_acquired = false; }
    bool acquired() const { return m_acquired; }

    const ReffedSelfChunk& m_ref;
    bool m_acquired = false;
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
    { }

    struct Info
    {
        std::size_t written = 0;
        std::size_t read = 0;
        std::size_t count = 0;
        void clear() { written = 0; read = 0; }
    };

    static Info latchInfo();

    virtual bool insert(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper& clipper)
    {
        if (clipper.insert(*this)) ref(climber);
        return m_chunk->insert(cell, climber);
    }

    void ref(const NewClimber& climber);
    void unref(Origin o);

    SelfChunk& chunk() { return *m_chunk; }

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

    virtual void init() override
    {
        assert(m_tubes.empty());
        m_tubes.resize(m_pointsAcross * m_pointsAcross);
        m_acquired = false;
    }

    virtual bool insert(Cell::PooledNode& cell, NewClimber& climber) override
    {
        const Xyz& pk(climber.pointKey().position());
        const std::size_t i(
                (pk.y % m_pointsAcross) * m_pointsAcross +
                (pk.x % m_pointsAcross));

        assert(i < m_tubes.size());
        return m_tubes[i].insert(climber, cell).done();
    }

    virtual ReffedSelfChunk& step(const Cell::PooledNode& cell) override
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), cell->point()));
        return *m_children.at(toIntegral(dir));
    }

private:
    virtual Cells acquire() override
    {
        Cells cells(m_ref.pointPool().cellPool());

        for (auto& tube : m_tubes)
        {
            for (auto& inner : tube) cells.push(std::move(inner.second));
        }

        m_tubes.clear();
        m_acquired = true;
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

    virtual bool insert(Cell::PooledNode& cell, NewClimber& climber) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const Xyz& pk(climber.pointKey().position());
        return m_tubes[pk.y][pk.x].insert(climber, cell).done();
    }

    virtual ReffedSelfChunk& step(const Cell::PooledNode& cell) override
    {
        return m_child;
    }

private:
    virtual Cells acquire() override
    {
        Cells cells(m_ref.pointPool().cellPool());

        for (auto& y : m_tubes)
        {
            for (auto& x : y.second)
            {
                Tube& t(x.second);
                for (auto& inner : t) cells.push(std::move(inner.second));
            }
        }

        m_tubes.clear();
        m_acquired = true;
        return cells;
    }

    std::mutex m_mutex;
    std::map<uint64_t, std::map<uint64_t, Tube>> m_tubes;

    ReffedSelfChunk m_child;
};

} // namespace entwine

