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

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
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

class Chunk;

class ReffedChunk
{
public:
    ReffedChunk(
            const ChunkKey& key,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            Hierarchy& hierarchy);

    ReffedChunk(const ReffedChunk& o);
    ~ReffedChunk();

    struct Info
    {
        std::size_t written = 0;
        std::size_t read = 0;
        std::size_t count = 0;
        std::size_t reffed = 0;
        void clear() { written = 0; read = 0; }
    };

    bool insert(Cell::PooledNode& cell, const Key& key, Clipper& clipper);

    void ref(Clipper& clipper);
    void unref(Origin o);
    bool empty();

    Chunk& chunk() { assert(m_chunk); return *m_chunk; }

    const ChunkKey& key() const { return m_key; }
    const Metadata& metadata() const { return m_metadata; }
    const arbiter::Endpoint& out() const { return m_out; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }
    PointPool& pointPool() const { return m_pointPool; }
    Hierarchy& hierarchy() const { return m_hierarchy; }

    static Info latchInfo();

private:
    ChunkKey m_key;
    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;
    Hierarchy& m_hierarchy;

    std::mutex m_mutex;
    std::unique_ptr<Chunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

class Chunk
{
public:
    Chunk(const ReffedChunk& ref)
        : m_ref(ref)
        , m_overflow(m_ref.pointPool().cellPool())
        , m_pointsAcross(1UL << m_ref.metadata().structure().body())
    {
        assert(m_ref.key().depth() < m_ref.metadata().structure().tail());

        init();

        m_children.reserve(dirEnd());
        for (std::size_t d(0); d < dirEnd(); ++d)
        {
            const ChunkKey key(m_ref.key().getStep(toDir(d)));
            m_children.emplace_back(
                    key,
                    m_ref.out(),
                    m_ref.tmp(),
                    m_ref.pointPool(),
                    m_ref.hierarchy());

            m_hasChildren = m_hasChildren || m_ref.hierarchy().get(key.get());
        }
    }

    void init()
    {
        assert(!m_tubes);
        m_tubes = makeUnique<std::vector<Tube>>(
                m_pointsAcross * m_pointsAcross);
        m_remote = false;
    }

    bool insert(const Key& key, Cell::PooledNode& cell, Clipper& clipper);

    ReffedChunk& step(const Point& p)
    {
        const Dir dir(getDirection(m_ref.key().bounds().mid(), p));
        return m_children[toIntegral(dir)];
    }

    bool terminus()
    {
        // Make sure we don't early-return here - need to traverse all children.
        bool result(true);
        for (auto& c : m_children) if (!c.empty()) result = false;
        return result;
    }

    CountedCells acquire()
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

        if (!m_overflow.empty())
        {
            assert(!m_hasChildren);
            cells.np += m_overflow.size();
            cells.stack.pushBack(std::move(m_overflow));
        }

        m_remote = true;

        return cells;
    }

    bool remote() const { return m_remote; }

private:
    bool insertNative(const Key& key, Cell::PooledNode& cell)
    {
        const Xyz& pos(key.position());
        const std::size_t i(
                (pos.y % m_pointsAcross) * m_pointsAcross +
                (pos.x % m_pointsAcross));

        assert(i < m_tubes->size());
        return (*m_tubes)[i].insert(key, cell);
    }

    const ReffedChunk& m_ref;
    bool m_remote = false;

    std::mutex m_overflowMutex;
    bool m_hasChildren = false;
    Cell::PooledStack m_overflow;
    std::stack<Key> m_keys;

    const std::size_t m_pointsAcross;
    std::unique_ptr<std::vector<Tube>> m_tubes;

    std::vector<ReffedChunk> m_children;
};

} // namespace entwine

