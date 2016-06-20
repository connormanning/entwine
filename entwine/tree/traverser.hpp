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
#include <set>

#include <pdal/PointTable.hpp>
#include <pdal/PointView.hpp>

#include <entwine/types/bbox.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Branch
{
public:
    Branch() : m_children() { }

    template<typename F> void recurse(const std::size_t depth, const F& f) const
    {
        if (m_children.size())
        {
            for (const auto& c : m_children)
            {
                f(c.first, depth);
                c.second.recurse(depth + 1, f);
            }
        }
    }

    Branch& operator[](const Id& id) { return m_children[id]; }
    const Branch& operator[](const Id& id) const { return m_children.at(id); }

private:
    std::map<Id, Branch> m_children;
};

class Traverser
{
public:
    Traverser(const Metadata& metadata, const std::set<Id>& ids)
        : m_metadata(metadata)
        , m_structure(m_metadata.structure())
        , m_ids(ids)
    { }

    template<typename F> void go(const F& f)
    {
        if (m_structure.hasCold()) go(f, ChunkState(m_metadata));
    }

    template<typename F> void tree(const F& f)
    {
        if (m_structure.hasCold()) tree(f, ChunkState(m_metadata));
    }

private:
    template<typename F>
    void recurse(const F& f, const ChunkState& chunkState)
    {
        const auto call([this, &f, &chunkState](Dir dir = Dir::swd)
        {
            const ChunkState nextState(chunkState.getChunkClimb(dir));
            f(nextState, m_ids.count(nextState.chunkId()));
        });

        if (!chunkState.sparse())
        {
            for (std::size_t i(0); i < dirHalfEnd(); ++i) call(toDir(i));
        }
        else call();
    }

    template<typename F>
    void go(
            const F& f,
            const ChunkState& chunkState,
            bool exists = true)
    {
        auto next([this, &f](const ChunkState& chunkState, bool exists)
        {
            go(f, chunkState, exists);
        });

        if (
                chunkState.depth() < m_structure.coldDepthBegin() ||
                f(chunkState, exists))
        {
            recurse(next, chunkState);
        }
    }

    template<typename F> void tree(const F& f, const ChunkState& chunkState)
    {
        if (chunkState.depth() < m_structure.coldDepthBegin())
        {
            auto next([this, &f](const ChunkState& chunkState, bool exists)
            {
                if (exists) tree(f, chunkState);
            });

            recurse(next, chunkState);
        }
        else if (chunkState.depth() == m_structure.coldDepthBegin())
        {
            auto branch(makeUnique<Branch>());
            buildBranch(*branch, chunkState);
            f(std::move(branch));
        }
        else
        {
            throw std::runtime_error("Invalid nominal chunk depth");
        }
    }

    void buildBranch(Branch& branch, const ChunkState& chunkState)
    {
        auto next([this, &branch](const ChunkState& chunkState, bool exists)
        {
            if (exists)
            {
                buildBranch(branch[chunkState.chunkId()], chunkState);
            }
        });

        recurse(next, chunkState);
    }

    const Metadata& m_metadata;
    const Structure& m_structure;
    const std::set<Id> m_ids;
};

} // namespace entwine

