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

#include <cstddef>
#include <memory>
#include <vector>

#include <pdal/PointView.hpp>

#include <entwine/tree/builder.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/pool.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Traverser;
class Tile;
class Tiler;

class Above
{
public:
    Above(
            const Id& chunkId,
            const BBox& bbox,
            const Schema& schema,
            std::size_t delta)
        : m_chunkId(chunkId)
        , m_bbox(bbox)
        , m_schema(schema)
        , m_delta(delta)
        , m_segments()
        , m_here(false)
    { }

    virtual ~Above()
    {
        // TODO If this is a transformation, smash the segments together and
        // then write out this chunk.
    }

    using Map = std::map<BBox, std::unique_ptr<Above>>;
    using MapVal = Map::value_type;
    using Set = std::set<Above*>;

    const Id& chunkId() const { return m_chunkId; }
    bool here() const { return m_here; }

    virtual void populate(std::unique_ptr<std::vector<char>> data);

    const std::vector<char>* data(const BBox& bbox) const
    {
        if (m_segments.count(bbox)) return &m_segments.at(bbox);
        else return nullptr;
    }

protected:
    const Id m_chunkId;
    const BBox m_bbox;
    const Schema& m_schema;
    const std::size_t m_delta;
    std::map<BBox, std::vector<char>> m_segments;
    bool m_here;
};

class Base : public Above
{
public:
    Base(const Tiler& tiler);

    virtual ~Base() override
    {
        // TODO Reserialize base, if this is a transformation.  Something like:
        /*
        const Schema celledSchema(BaseChunk::makeCelled(m_builder.schema()));
        auto toWrite(Compression::compress(*m_baseChunk, celledSchema));

        Chunk::pushTail(
                *toWrite,
                Chunk::Tail(
                    m_baseChunk->size() / celledSchema.pointSize(),
                    Chunk::Contiguous));

        m_outEndpoint->putSubpath(
                m_builder.structure().baseIndexBegin().str(),
                *toWrite);
        */
    }

private:
    virtual void populate(std::unique_ptr<std::vector<char>> data) override;

    const Structure& m_structure;
};

class Tile
{
    using Belows = std::map<Id, std::unique_ptr<std::size_t>>;
    using Below = Belows::value_type;
    using Aboves = std::map<Above*, std::unique_ptr<std::size_t>>;

public:
    Tile(const BBox& bbox, const Schema& schema, Above::Map& aboves)
        : m_bbox(bbox)
        , m_schema(schema)
        , m_aboves(getContainingFrom(m_bbox, aboves))
        , m_belows()
        , m_data()
        , m_owned(false)
    { }

    ~Tile()
    {
        // TODO If this is a transformation, write out each of the Belows.
        // Something like:
        /*
        if (m_outEndpoint)
        {
            auto toWrite(Compression::compress(data, schema));

            Chunk::pushTail(
                    *toWrite,
                    Chunk::Tail(
                        data.size() / schema.pointSize(),
                        Chunk::Contiguous)); // TODO Get from Structure.

            m_outEndpoint->putSubpath(id.str(), *toWrite);
        }
        */
    }

    void await(const Id& id) { m_belows[id] = nullptr; }
    void insert(const Id& id, std::unique_ptr<std::vector<char>> data)
    {
        m_belows.at(id).reset(new std::size_t(m_data.size()));
        m_data.insert(m_data.begin(), data->begin(), data->end());
    }

    // Returns true if the caller is cleared for processing this Tile.  If
    // the result is false, the caller should not process this Tile.
    bool acquire()
    {
        if (!m_owned && allHere())
        {
            m_owned = true;
            return true;
        }
        else
        {
            return false;
        }
    }

    bool allHere() const
    {
        auto aboveHere([](const Above* a) { return a->here(); });
        auto belowHere([](const Below& b) { return b.second.get(); });
        return
            std::all_of(m_aboves.begin(), m_aboves.end(), aboveHere) &&
            std::all_of(m_belows.begin(), m_belows.end(), belowHere);
    }

    void process(const TileFunction& f);

    bool references(Above& above) const { return m_aboves.count(&above); }

private:
    Above::Set getContainingFrom(const BBox& bbox, Above::Map& aboves) const;

    const BBox m_bbox;
    const Schema& m_schema;

    Above::Set m_aboves;
    Belows m_belows;

    std::vector<char> m_data;
    bool m_owned;
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

    void go(const TileFunction& f);

    const Builder& builder() const { return m_builder; }
    const Schema* wantedSchema() const { return m_wantedSchema; }
    std::size_t sliceDepth() const { return m_sliceDepth; }

    const Schema& activeSchema() const
    {
        return m_wantedSchema ? *m_wantedSchema : m_builder.schema();
    }

private:
    void init(double maxArea);

    void insertAbove(
            const TileFunction& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox);

    void spawnTile(
            const TileFunction& f,
            const Id& chunkId,
            const BBox& bbox,
            bool exists);

    void buildTile(
            const TileFunction& f,
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox);

    void awaitAndAcquire(
            const TileFunction& f,
            const Id& chunkId,
            Tile& tile);

    void maybeProcess(const TileFunction& f);

    std::unique_ptr<std::vector<char>> acquire(const Id& chunkId);

    using TileMap = std::map<BBox, std::unique_ptr<Tile>>;

    const Builder& m_builder;

    std::unique_ptr<Traverser> m_traverser;
    std::unique_ptr<arbiter::Endpoint> m_outEndpoint;
    mutable Pool m_pool;
    mutable std::mutex m_mutex;

    std::unique_ptr<Base> m_baseChunk;
    std::size_t m_sliceDepth;

    const Schema* const m_wantedSchema;

    std::map<BBox, std::unique_ptr<Above>> m_aboves;
    TileMap m_tiles;

    std::unique_ptr<BBox> m_current;
    std::set<BBox> m_processing;

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

} // namespace entwine

