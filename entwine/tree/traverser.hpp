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

#include <pdal/PointTable.hpp>
#include <pdal/PointView.hpp>

#include <entwine/tree/builder.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/util/pool.hpp>

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

/*
class Traverser
{
public:
    // TODO This class really only works for hybrid trees right now.  It should
    // be genericized similar to Climber.
    Traverser(const Builder& builder, const std::set<Id>* ids = nullptr)
        , m_structure(m_builder.structure())
        , m_ids(ids ? *ids : m_builder.registry().ids())
    { }

    template<typename F> void go(const F& f)
    {
        if (m_structure.hasCold())
        {
            go(
                    f,
                    m_structure.nominalChunkIndex(),
                    m_structure.nominalChunkDepth(),
                    m_builder.bbox());
        }
    }

    template<typename F> void tree(const F& f)
    {
        if (m_structure.hasCold())
        {
            tree(
                    f,
                    m_structure.nominalChunkIndex(),
                    m_structure.nominalChunkDepth(),
                    m_builder.bbox());
        }
    }

private:
    template<typename F>
    void recurse(
            const F& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox)
    {
        Id nextId(chunkId << m_structure.dimensions());
        nextId.incSimple();

        if (++depth <= m_structure.sparseDepthBegin())
        {
            f(nextId, depth, bbox.getSwd(true), m_ids.count(nextId));

            nextId += m_structure.baseChunkPoints();
            f(nextId, depth, bbox.getSed(true), m_ids.count(nextId));

            nextId += m_structure.baseChunkPoints();
            f(nextId, depth, bbox.getNwd(true), m_ids.count(nextId));

            nextId += m_structure.baseChunkPoints();
            f(nextId, depth, bbox.getNed(true), m_ids.count(nextId));
        }
        else
        {
            f(nextId, depth, bbox, m_ids.count(nextId));
        }
    }

    template<typename F>
    void go(
            const F& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox,
            bool exists = true)
    {
        auto next([this, &f](
                const Id& chunkId,
                std::size_t depth,
                const BBox& bbox,
                bool exists)
        {
            go(f, chunkId, depth, bbox, exists);
        });

        if (
                depth < m_structure.coldDepthBegin() ||
                f(chunkId, depth, bbox, exists))
        {
            recurse(next, chunkId, depth, bbox);
        }
    }

    template<typename F> void tree(
            const F& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox)
    {
        if (depth < m_structure.coldDepthBegin())
        {
            auto next([this, &f](
                    const Id& chunkId,
                    std::size_t depth,
                    const BBox& bbox,
                    bool exists)
            {
                if (exists) tree(f, chunkId, depth, bbox);
            });

            recurse(next, chunkId, depth, bbox);
        }
        else if (depth == m_structure.coldDepthBegin())
        {
            std::unique_ptr<Branch> branch(new Branch());
            buildBranch(*branch, chunkId, depth, bbox);
            f(std::move(branch));
        }
        else
        {
            throw std::runtime_error("Invalid nominal chunk depth");
        }
    }

    void buildBranch(
            Branch& branch,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox)
    {
        auto next([this, &branch](
                const Id& chunkId,
                std::size_t depth,
                const BBox& bbox,
                bool exists)
        {
            if (exists) buildBranch(branch[chunkId], chunkId, depth, bbox);
        });

        recurse(next, chunkId, depth, bbox);
    }

    const Builder& m_builder;
    const Structure& m_structure;
    const std::set<Id> m_ids;
};
*/

} // namespace entwine

