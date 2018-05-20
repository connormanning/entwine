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

    virtual bool insert(const Key& key, Cell::PooledNode& cell) = 0;
    virtual ReffedSelfChunk& step(const Point& p) = 0;

protected:
    virtual CountedCells acquire() = 0;
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
        , m_hasChildren(false)
        , m_overflow(m_pointPool.cellPool())
    {
        const auto& s(m_metadata.structure());

        const std::size_t pointsAcross(1UL << s.body());
        const float size(pointsAcross * pointsAcross);
        m_limit = size * 0.25;

        m_overflowDepth = s.body() + (s.tail() - s.body()) / 2;
    }

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
        if (m_chunk->insert(key, cell)) return true;
        // return false;
        if (m_key.depth() < m_overflowDepth) return false;

        std::lock_guard<std::mutex> lock(m_overflowMutex);
        if (m_hasChildren) return false;

        m_overflow.push(std::move(cell));
        m_keys.push(key);

        assert(m_overflow.size() == m_keys.size());

        if (m_overflow.size() <= m_limit) return true;

        m_hasChildren = true;

        while (!m_overflow.empty())
        {
            auto curCell(m_overflow.popOne());
            Key curKey(m_keys.top());
            m_keys.pop();

            curKey.step(curCell->point());
            if (!m_chunk->step(curCell->point()).insert(curCell, curKey, clipper))
            {
                throw std::runtime_error("Invalid overflow");
            }

            assert(m_overflow.size() == m_keys.size());
        }

        assert(m_overflow.empty());
        assert(m_keys.empty());

        return true;
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

    std::mutex m_overflowMutex;
    bool m_hasChildren;
    Cell::PooledStack m_overflow;
    std::stack<Key> m_keys;
    std::size_t m_limit;
    std::size_t m_overflowDepth;
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

    virtual bool insert(const Key& key, Cell::PooledNode& cell) override
    {
        const Xyz& pos(key.position());
        const std::size_t i(
                (pos.y % m_pointsAcross) * m_pointsAcross +
                (pos.x % m_pointsAcross));

        assert(i < m_tubes.size());
        return m_tubes[i].insert(key, cell);
    }

    virtual ReffedSelfChunk& step(const Point& p) override
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return *m_children.at(toIntegral(dir));
    }

private:
    virtual CountedCells acquire() override
    {
        CountedCells cells(m_ref.pointPool().cellPool());

        for (auto& tube : m_tubes)
        {
            for (auto& inner : tube)
            {
                cells.np += inner.second->size();
                cells.stack.push(std::move(inner.second));
            }
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
    virtual CountedCells acquire() override
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
        m_acquired = true;
        return cells;
    }

    std::mutex m_mutex;
    std::map<uint64_t, std::map<uint64_t, Tube>> m_tubes;

    ReffedSelfChunk m_child;
};

} // namespace entwine

