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

#include <chrono>
#include <mutex>
#include <set>
#include <thread>

#include <pdal/PointTable.hpp>
#include <pdal/PointView.hpp>

#include <entwine/types/defs.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Branch;

class BranchNode
{
    friend class Branch;

public:
    BranchNode(const Id& id, std::size_t depth)
        : m_id(id)
        , m_depth(depth)
        , m_children()
    { }

    template<typename F> void recurse(std::size_t depth, const F& f) const
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

    BranchNode& operator[](const Id& id)
    {
        if (!m_children.count(id))
        {
            m_children.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(id),
                    std::forward_as_tuple(id, m_depth + 1));
        }

        return m_children.at(id);
    }

    const BranchNode& operator[](const Id& id) const
    {
        return m_children.at(id);
    }

    const std::map<Id, BranchNode>& children() const { return m_children; }
    const Id& id() const { return m_id; }
    std::size_t depth() const { return m_depth; }

private:
    const Id m_id;
    const std::size_t m_depth;
    std::map<Id, BranchNode> m_children;
};

class Traverser;

class Branch
{
    friend class Traverser;

public:
    Branch(const Id& id, std::size_t startDepth)
        : m_node(id, startDepth)
    { }

    template<typename F> void recurse(const F& f) const
    {
        f(m_node.id(), m_node.depth());
        m_node.recurse(m_node.depth() + 1, f);
    }

    template<typename F> void recurse(Pool& pool, const F& f) const
    {
        std::mutex m;

        std::queue<const BranchNode*> ready;
        std::set<const BranchNode*> blocked;

        auto pop([&]()->const BranchNode*
        {
            std::lock_guard<std::mutex> lock(m);

            if (ready.empty()) return nullptr;

            const BranchNode* c(ready.front());
            ready.pop();
            return c;
        });

        ready.push(&m_node);

        std::set<Id> outstanding;

        while (ready.size() || blocked.size())
        {
            if (const BranchNode* current = pop())
            {
                for (const auto& p : current->children())
                {
                    blocked.insert(&p.second);
                }

                pool.add([&, current]()
                {
                    {
                        std::lock_guard<std::mutex> lock(m);
                        outstanding.insert(current->id());
                    }

                    f(current->id(), current->depth());

                    std::lock_guard<std::mutex> lock(m);
                    outstanding.erase(current->id());
                    for (const auto& p : current->children())
                    {
                        blocked.erase(&p.second);
                        ready.push(&p.second);
                    }
                });
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        std::cout << "Awaiting..." << std::endl;
        pool.await();
        std::cout << "\tAwaited" << std::endl;
    }

    const Id& id() const { return m_node.id(); }

private:
    BranchNode& node() { return m_node; }

    BranchNode m_node;
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
        const auto call([this, &f, &chunkState](Dir dir)
        {
            const ChunkState nextState(chunkState.getChunkClimb(dir));
            f(nextState, m_ids.count(nextState.chunkId()));
        });

        if (!chunkState.sparse())
        {
            for (std::size_t i(0); i < dirHalfEnd(); ++i) call(toDir(i));
        }
        else call(Dir::swd);
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
        const std::size_t coldDepth(m_structure.coldDepthBegin());

        if (chunkState.depth() < coldDepth)
        {
            auto next([this, &f](const ChunkState& chunkState, bool exists)
            {
                if (exists) tree(f, chunkState);
            });

            recurse(next, chunkState);
        }
        else if (chunkState.depth() == coldDepth)
        {
            Branch branch(chunkState.chunkId(), coldDepth);
            buildBranch(branch.node(), chunkState);
            f(branch);
        }
        else
        {
            throw std::runtime_error("Invalid nominal chunk depth");
        }
    }

    void buildBranch(BranchNode& node, const ChunkState& chunkState)
    {
        auto next([this, &node](const ChunkState& chunkState, bool exists)
        {
            if (exists)
            {
                buildBranch(node[chunkState.chunkId()], chunkState);
            }
        });

        recurse(next, chunkState);
    }

    const Metadata& m_metadata;
    const Structure& m_structure;
    const std::set<Id> m_ids;
};

} // namespace entwine

