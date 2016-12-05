#pragma once

#include <memory>
#include <vector>

#include <pdal/PointView.hpp>
#include <pdal/StageFactory.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/point.hpp>

namespace test
{

class Octree;

class Traversal
{
public:
    Traversal(
            const Octree& octree,
            std::size_t origin,
            pdal::PointView& view,
            std::size_t index,
            entwine::Point point);

    const pdal::PointView& view() const { return m_view; }
    const entwine::Point& point() const { return m_point; }
    std::size_t index() const { return m_index; }
    std::size_t depth() const { return m_depth; }

    const Octree& octree() const { return m_octree; }
    std::size_t depthBegin() const;
    std::size_t depthEnd() const;
    const entwine::Bounds& bounds() const { return m_bounds; }

    entwine::Dir dir() const
    {
        return getDirection(m_bounds.mid(), m_point);
    }

    void next(entwine::Dir dir)
    {
        m_bounds.go(dir);
        ++m_depth;
    }

private:
    const Octree& m_octree;
    const std::size_t m_origin;
    const pdal::PointView& m_view;
    const std::size_t m_index;
    const entwine::Point m_point;

    entwine::Bounds m_bounds;
    std::size_t m_depth = 0;
};

using Traversals = std::vector<Traversal>;
using Slot = std::unique_ptr<Traversals>;
using Query = std::vector<const Traversal*>;

class Node
{
public:
    bool insert(Traversal& t);
    bool insert(Slot slot);
    bool empty() const { return !m_slot; }
    const entwine::Point& point() const
    {
        if (empty() || m_slot->empty())
        {
            throw std::runtime_error("Invalid slot");
        }

        return m_slot->front().point();
    }

    Query query(
            const entwine::Bounds& bounds,
            const entwine::Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd) const
    {
        Query q;
        query(q, bounds, 0, queryBounds, depthBegin, depthEnd);
        return q;
    }

private:
    void query(
            Query& q,
            const entwine::Bounds& bounds,
            std::size_t depth,
            const entwine::Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd) const
    {
        if (depthEnd && depth >= depthEnd) return;
        if (!bounds.overlaps(queryBounds)) return;

        auto next([&]()
        {
            for (const auto& c : m_children)
            {
                const auto dir(c.first);
                const auto& child(*c.second);

                child.query(
                    q,
                    bounds.get(dir),
                    depth + 1,
                    queryBounds,
                    depthBegin,
                    depthEnd);
            }
        });

        if (depth >= depthBegin)
        {
            if (empty()) return;
            if (queryBounds.contains(point()))
            {
                for (const auto& t : *m_slot) q.push_back(&t);
            }
        }

        next();
    }

    Slot m_slot;
    std::map<entwine::Dir, std::unique_ptr<Node>> m_children;
};

class Octree
{
public:
    Octree(
            entwine::Bounds bounds,
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0)
        : m_bounds(bounds)
        , m_delta(nullptr)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd)
    { }

    Octree(
            entwine::Bounds bounds,
            const entwine::Delta& delta,
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0)
        : m_bounds(bounds)
        , m_delta(&delta)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd)
    { }

    void insert(std::string path);

    const entwine::Bounds& bounds() const { return m_bounds; }
    std::size_t depthBegin() const { return m_depthBegin; }
    std::size_t depthEnd() const { return m_depthEnd; }

    Query query(std::size_t depth) const
    {
        return query(depth, depth + 1);
    }

    Query query(std::size_t depthBegin, std::size_t depthEnd) const
    {
        return query(m_bounds, depthBegin, depthEnd);
    }

    Query query(
            const entwine::Bounds& queryBounds,
            std::size_t depth) const
    {
        return query(queryBounds, depth, depth + 1);
    }

    Query query(
            const entwine::Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd) const
    {
        return m_root.query(m_bounds, queryBounds, depthBegin, depthEnd);
    }

    struct Data
    {
        pdal::PointViewPtr view;
        std::size_t inserts;
        std::size_t outOfBounds;
    };

    const std::vector<Data>& data() const { return m_data; }
    std::size_t inserts() const
    {
        std::size_t n(0);
        for (const auto& d : m_data) n += d.inserts;
        return n;
    }
    std::size_t outOfBounds() const
    {
        std::size_t n(0);
        for (const auto& d : m_data) n += d.outOfBounds;
        return n;
    }

private:
    const entwine::Bounds m_bounds;
    const entwine::Delta* m_delta;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;

    pdal::StageFactory m_stageFactory;
    std::vector<Data> m_data;

    Node m_root;
};

} // namespace test

