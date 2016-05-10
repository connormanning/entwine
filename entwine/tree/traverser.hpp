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
#include <entwine/types/structure.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/compression/util.hpp>

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
    // TODO This class really only works for hybrid trees right now.  It should
    // be genericized similar to Climber.
    Traverser(const Builder& builder, const std::set<Id>* ids = nullptr);

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

class Tiler
{
public:
    Tiler(
            const Builder& builder,
            std::size_t threads,
            double maxArea,
            const Schema* wantedSchema = nullptr);

    Tiler(
            const Builder& builder,
            const arbiter::Endpoint& output,
            std::size_t threads,
            double maxArea);

    typedef std::function<bool(pdal::PointView& view, BBox bbox)> TileFunction;

    void go(const TileFunction& f);

    typedef std::map<Id, std::unique_ptr<std::vector<char>>> ChunkData;
    typedef std::map<BBox, ChunkData> Tiles;

private:
    void init(double maxArea);

    void insert(
            const TileFunction& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox,
            bool exists);

    void maybeProcess(
            const TileFunction& f,
            std::size_t depth,
            const BBox& base);

    void actuallyProcess(const TileFunction& f, const BBox& base);

    // True only if all overlapping tiles above this base have been fetched and
    // all tiles within this base have been fetched.
    bool allHere(const BBox& base) const;

    // Takes a tile from above the slice-depth and splits it down to the
    // actual tile bounds (for the tiles that current have corresponding
    // outstanding data), calling maybeProcess on each actual tile.  Caller
    // must not hold a lock on m_mutex.
    void explodeAbove(const TileFunction& f, const BBox& superTile);

    std::unique_ptr<std::vector<char>> acquire(const Id& chunkId)
    {
        const std::string path(m_builder.structure().maybePrefix(chunkId));
        return Storage::ensureGet(m_builder.outEndpoint(), path);
    }

    const Builder& m_builder;

    Traverser m_traverser;
    std::unique_ptr<arbiter::Endpoint> m_outEndpoint;
    Pool m_pool;
    mutable std::mutex m_mutex;

    std::unique_ptr<std::vector<char>> m_baseChunk;
    std::unique_ptr<BBox> m_current;
    Tiles m_above;
    Tiles m_tiles;
    std::size_t m_sliceDepth;

    std::set<BBox> m_processing;

    const Schema* m_wantedSchema;
};

class TiledPointTable : public pdal::BasePointTable
{
    friend class Tiler;

public:
    TiledPointTable(
            const Schema& schema,
            const BBox& bbox,
            Tiler::Tiles& tiles,
            Tiler::Tiles& above,
            std::vector<char>* baseChunk,
            std::mutex& mutex);

    std::size_t size() const { return m_points.size(); }

private:
    void addAll(std::vector<char>& chunk);
    void addInRange(std::vector<char>& chunk, const BBox& bbox);

    virtual void setFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            const void* pos)
    {
        const pdal::Dimension::Detail& dimDetail(
                *m_schema.pdalLayout().dimDetail(dimId));
        const char* src(static_cast<const char*>(pos));
        char* dst(getPoint(i) + dimDetail.offset());

        std::copy(src, src + dimDetail.size(), dst);
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            void* pos) const
    {
        const pdal::Dimension::Detail& dimDetail(
                *m_schema.pdalLayout().dimDetail(dimId));
        const char* src(getPoint(i) + dimDetail.offset());

        std::copy(src, src + dimDetail.size(), static_cast<char*>(pos));
    }

    virtual pdal::PointId addPoint()
    {
        throw std::runtime_error("Cannot add points to a TiledPointTable");
    }

    virtual char* getPoint(pdal::PointId i)         { return m_points.at(i); }
    const   char* getPoint(pdal::PointId i) const   { return m_points.at(i); }

    const Schema& m_schema;
    std::vector<char*> m_points;
};

class SizedPointView : public pdal::PointView
{
public:
    template<typename Table>
    SizedPointView(Table& table)
        : PointView(table)
    {
        m_size = table.size();
        for (std::size_t i(0); i < m_size; ++i) m_index.push_back(i);
    }
};

class VectorPointTable : public pdal::BasePointTable
{
public:
    VectorPointTable(const Schema& schema, std::vector<char>& data)
        : BasePointTable(schema.pdalLayout())
        , m_schema(schema)
        , m_data(data)
    { }

    std::size_t size() const { return m_data.size() / m_schema.pointSize(); }

    virtual char* getPoint(pdal::PointId i)
    {
        return m_data.data() + (i * m_schema.pointSize());
    }

private:
    virtual void setFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            const void* pos)
    {
        throw std::runtime_error("VectorPointTable is read-only");
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            void* pos) const
    {
        const pdal::Dimension::Detail& dimDetail(
                *m_schema.pdalLayout().dimDetail(dimId));
        const char* src(getPoint(i) + dimDetail.offset());

        std::copy(src, src + dimDetail.size(), static_cast<char*>(pos));
    }

    virtual pdal::PointId addPoint()
    {
        throw std::runtime_error("Cannot add points to a TiledPointTable");
    }

    const char* getPoint(pdal::PointId i) const
    {
        return m_data.data() + (i * m_schema.pointSize());
    }

    const Schema& m_schema;
    std::vector<char>& m_data;
};

} // namespace entwine

