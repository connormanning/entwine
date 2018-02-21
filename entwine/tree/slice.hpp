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
#include <mutex>

#include <entwine/tree/key.hpp>
#include <entwine/tree/new-chunk.hpp>
#include <entwine/tree/new-climber.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Slice
{
public:
    Slice(
            const Metadata& metadata,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            uint64_t depth,
            std::size_t chunksAcross,
            std::size_t pointsAcross)
        : m_metadata(metadata)
        , m_out(out)
        , m_tmp(tmp)
        , m_pointPool(pointPool)
        , m_depth(depth)
        , m_contiguous(m_depth < m_metadata.structure().tail())
        , m_chunksAcross(chunksAcross)
        , m_pointsAcross(pointsAcross)
        , m_chunks(m_chunksAcross * m_chunksAcross)
    { }

    Tube::Insertion insert(
            Cell::PooledNode& cell,
            const NewClimber& climber,
            NewClipper& clipper)
    {
        const auto& ck(climber.chunkKey());

        const std::size_t i(ck.y * m_chunksAcross + ck.x);
        ReffedChunk& rc(m_chunks.at(i));

        if (clipper.insert(climber.depth(), ck.x, ck.y))
        {
            rc.ref(*this, climber);
        }

        return rc.chunk().insert(cell, climber);
    }

    void clip(uint64_t x, uint64_t y, uint64_t o)
    {
        const std::size_t i(y * m_chunksAcross + x);
        ReffedChunk& rc(m_chunks.at(i));
        rc.unref(*this, x, y, o);
    }

    PointPool& pointPool() const { return m_pointPool; }
    uint64_t depth() const { return m_depth; }

private:
    std::unique_ptr<NewChunk> create(const Bounds& b) const
    {
        if (m_contiguous)
        {
            return makeUnique<NewContiguousChunk>(b, m_pointsAcross);
        }
        else
        {
            return makeUnique<NewMappedChunk>(b, m_pointsAcross);
        }
    }

    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;

    const uint64_t m_depth;
    const bool m_contiguous;
    std::size_t m_chunksAcross;
    std::size_t m_pointsAcross;

    class ReffedChunk
    {
    public:
        void ref(const Slice& s, const NewClimber& climber)
        {
            const auto& ck(climber.chunkKey());
            const auto o(climber.origin());

            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_refs.count(o))
            {
                m_refs[o] = 1;

                if (!m_chunk)
                {
                    m_chunk = s.create(ck.bounds());
                    if (m_np)
                    {
                        Cells cells(s.read(ck.x, ck.y));
                        assert(cells.size() == m_np);

                        NewClimber c(climber);

                        while (!cells.empty())
                        {
                            auto cell(cells.popOne());
                            c.init(cell->point(), s.depth());
                            m_chunk->insert(cell, c);
                        }
                    }
                }
            }
            else ++m_refs[o];
        }

        void unref(const Slice& s, uint64_t x, uint64_t y, uint64_t o)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!--m_refs.at(o))
            {
                m_refs.erase(o);
                if (m_refs.empty())
                {
                    auto cells(m_chunk->acquire(s.pointPool()));

                    m_np = 0;
                    for (const Cell& cell : cells) m_np += cell.size();
                    s.write(x, y, m_chunk->bounds(), std::move(cells));

                    m_chunk.reset();
                }
            }
        }

        NewChunk& chunk() { return *m_chunk; }

    private:
        std::size_t m_np = 0;
        std::unique_ptr<NewChunk> m_chunk;
        std::map<Origin, std::size_t> m_refs;
        std::mutex m_mutex;
    };

    void write(
            uint64_t x,
            uint64_t y,
            const Bounds& bounds,
            Cells&& cells) const
    {
        m_metadata.storage().write(
                m_out,
                m_tmp,
                m_pointPool,
                filename(x, y),
                bounds,
                std::move(cells));
    }

    Cells read(uint64_t x, uint64_t y) const
    {
        return m_metadata.storage().read(
                m_out,
                m_tmp,
                m_pointPool,
                filename(x, y));
    }

    std::string filename(uint64_t x, uint64_t y) const
    {
        return std::to_string(m_depth) + '-' +
            std::to_string(x) + '-' + std::to_string(y);
    }

    std::vector<ReffedChunk> m_chunks;
};

} // namespace entwine

