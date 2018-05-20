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
#include <stack>
#include <utility>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/new-clipper.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

struct CountedCells
{
    CountedCells(Cell::Pool& pool) : stack(pool) { }
    Cells stack;
    uint64_t np = 0;
};

class SelfChunk
{
    friend class ReffedSelfChunk;

public:
    SelfChunk(const ReffedSelfChunk& ref);
    virtual ~SelfChunk() { assert(acquired()); }

    virtual ReffedSelfChunk& step(const Point& p) = 0;
    bool insert(const Key& key, Cell::PooledNode& cell, NewClipper& clipper);

protected:
    virtual bool insert(
            const Key& key,
            Cell::PooledNode& cell) = 0;

    CountedCells acquire()
    {
        CountedCells cells(doAcquire());
        m_acquired = true;

        if (!m_overflow.empty())
        {
            assert(!m_hasChildren);
            cells.np += m_overflow.size();
            cells.stack.pushBack(std::move(m_overflow));
        }
        return cells;
    }

    virtual CountedCells doAcquire() = 0;
    bool acquired() const { return m_acquired; }
    virtual void init() { m_acquired = false; }

    const ReffedSelfChunk& m_ref;
    bool m_acquired = false;

    std::mutex m_overflowMutex;
    bool m_hasChildren = false;
    Cell::PooledStack m_overflow;
    std::stack<Key> m_keys;
    std::size_t m_limit;
    std::size_t m_overflowDepth;
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

    bool insert(Cell::PooledNode& cell, const Key& key, NewClipper& clipper)
    {
        if (clipper.insert(*this)) ref(clipper);
        return m_chunk->insert(key, cell, clipper);
    }

    void ref(NewClipper& clipper);
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
        , m_tubes(makeUnique<std::vector<Tube>>(m_pointsAcross * m_pointsAcross))
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
        assert(!m_tubes);
        m_tubes = makeUnique<std::vector<Tube>>(m_pointsAcross * m_pointsAcross);
        m_acquired = false;
    }

    virtual bool insert(const Key& key, Cell::PooledNode& cell) override
    {
        const Xyz& pos(key.position());
        const std::size_t i(
                (pos.y % m_pointsAcross) * m_pointsAcross +
                (pos.x % m_pointsAcross));

        assert(i < m_tubes->size());
        return (*m_tubes)[i].insert(key, cell);
    }

    virtual ReffedSelfChunk& step(const Point& p) override
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return *m_children.at(toIntegral(dir));
    }

private:
    virtual CountedCells doAcquire() override
    {
        CountedCells cells(m_ref.pointPool().cellPool());

        for (auto& tube : *m_tubes)
        {
            for (auto& inner : tube)
            {
                cells.np += inner.second->size();
                cells.stack.push(std::move(inner.second));
            }
        }

        m_tubes.reset();
        return cells;
    }

    const std::size_t m_pointsAcross;
    std::unique_ptr<std::vector<Tube>> m_tubes;

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

    virtual bool insert(const Key& key, Cell::PooledNode& cell) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const Xyz& pos(key.position());
        return m_tubes[pos.y][pos.x].insert(key, cell);
    }

    virtual ReffedSelfChunk& step(const Point&) override
    {
        return m_child;
    }

private:
    virtual CountedCells doAcquire() override
    {
        CountedCells cells(m_ref.pointPool().cellPool());

        for (auto& y : m_tubes)
        {
            for (auto& x : y.second)
            {
                Tube& t(x.second);
                for (auto& inner : t)
                {
                    cells.np += inner.second->size();
                    cells.stack.push(std::move(inner.second));
                }
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

