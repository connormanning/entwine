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

#include <entwine/tree/new-chunk.hpp>
#include <entwine/tree/new-climber.hpp>
#include <entwine/tree/split-chunk.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Slice
{
    friend class ReffedChunk;

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
    {
        for (std::size_t i(0); i < m_chunksAcross * m_chunksAcross; ++i)
        {
            m_chunks.emplace_back(
                    SplitChunk::create(m_contiguous, m_chunksAcross));
        }
    }

    Tube::Insertion insert(
            Cell::PooledNode& cell,
            const NewClimber& climber,
            NewClipper& clipper)
    {
        const Xyz& ck(climber.chunkKey().position());
        const std::size_t i(ck.y * m_chunksAcross + ck.x);
        ReffedChunk& rc(m_chunks[i]->get(ck.z));

        if (clipper.insert(climber.depth(), ck)) rc.ref(*this, climber);

        return rc.chunk().insert(cell, climber);
    }

    void clip(const Xyz& p, uint64_t o)
    {
        const std::size_t i(p.y * m_chunksAcross + p.x);
        ReffedChunk& rc(m_chunks[i]->at(p.z));
        rc.unref(*this, p, o);
    }

    PointPool& pointPool() const { return m_pointPool; }
    uint64_t depth() const { return m_depth; }
    std::size_t np(const Xyz& p) const
    {
        const std::size_t i(p.y * m_chunksAcross + p.x);
        return m_chunks[i]->np(p.z);
    }

    void setNp(const Xyz& p, std::size_t np)
    {
        const std::size_t i(p.y * m_chunksAcross + p.x);
        m_chunks[i]->setNp(p.z, np);
    }

    struct Info
    {
        std::size_t written = 0;
        std::size_t read = 0;
        void clear()
        {
            written = 0;
            read = 0;
        }
    };

    static Info latchInfo();

private:
    std::unique_ptr<NewChunk> create() const
    {
        if (m_contiguous) return makeUnique<NewContiguousChunk>(m_pointsAcross);
        else return makeUnique<NewMappedChunk>(m_pointsAcross);
    }

    void write(const Xyz& p, Cells&& cells) const;
    Cells read(const Xyz& p) const;

    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;

    const uint64_t m_depth;
    const bool m_contiguous;
    const std::size_t m_chunksAcross;
    const std::size_t m_pointsAcross;

    std::vector<std::unique_ptr<SplitChunk>> m_chunks;
};

} // namespace entwine

