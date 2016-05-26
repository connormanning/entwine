/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/tiler.hpp>

#include <numeric>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/traverser.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

void Above::populate(std::unique_ptr<std::vector<char>> data)
{
    const std::size_t pointSize(m_schema.pointSize());
    const std::size_t numPoints(data->size() / pointSize);

    VectorPointTable table(m_schema, *data);
    pdal::PointRef pointRef(table, 0);

    Point p(0, 0, m_bbox.mid().z);
    BBox b;

    char* pos(data->data());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        pointRef.setPointId(i);

        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);

        // It's likely that the points will arrive in an order such that
        // many points in a row will belong to the same sub-box.
        if (!b.contains(p))
        {
            b = m_bbox;

            for (std::size_t d(0); d < m_delta; ++d)
            {
                b.go(getDirection(b.mid(), p), true);
            }
        }

        auto& segment(m_segments[b]);
        segment.insert(segment.end(), pos, pos + pointSize);
    }

    m_here = true;
}

void Base::populate(std::unique_ptr<std::vector<char>> data)
{
    const std::size_t pointSize(m_schema.pointSize());
    const std::size_t numPoints(data->size() / pointSize);

    VectorPointTable table(m_schema, *data);
    pdal::PointRef pointRef(table, 0);

    Point p(0, 0, m_bbox.mid().z);
    BBox b;
    Climber c(m_bbox, m_structure);

    char* pos(data->data());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        pointRef.setPointId(i);

        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);

        // It's likely that the points will arrive in an order such that
        // many points in a row will belong to the same sub-box.
        if (!b.contains(p))
        {
            c.reset();
            c.magnifyTo(p, m_delta);    // For the Base, delta = sliceDepth.
            b = c.bboxChunk();
        }

        auto& segment(m_segments[b]);
        segment.insert(segment.end(), pos, pos + pointSize);
    }

    m_here = true;
}

Base::Base(const Tiler& tiler)
    : Above(
            tiler.builder().structure().baseIndexBegin(),
            tiler.builder().bbox(),
            tiler.activeSchema(),
            tiler.sliceDepth())
    , m_structure(tiler.builder().structure())
{
    const Builder& builder(tiler.builder());
    const Schema celledSchema(BaseChunk::makeCelled(builder.schema()));

    const std::string path(builder.structure().maybePrefix(m_chunkId));
    auto cmp(Storage::ensureGet(builder.outEndpoint(), path));

    if (!cmp)
    {
        throw std::runtime_error("Could not acquire base: " + m_chunkId.str());
    }

    std::unique_ptr<Schema> celledWantedSchema;

    if (const Schema* wantedSchema = tiler.wantedSchema())
    {
        celledWantedSchema.reset(
                new Schema(
                    BaseChunk::makeCelled(*wantedSchema)));
    }

    const std::size_t numPoints(Chunk::popTail(*cmp).numPoints);
    std::cout << "Base points: " << numPoints << std::endl;
    auto data(
            Compression::decompress(
                *cmp,
                celledSchema,
                // TODO We're tossing information by not using the celled
                // version, so currently this is read-only - no transformations.
                // celledWantedSchema.get(),
                tiler.wantedSchema(),
                numPoints));

    populate(std::move(data));
}

Tiler::Tiler(
        const Builder& builder,
        const std::size_t threads,
        const double tileWidth,
        const Schema* wantedSchema)
    : m_builder(builder)
    , m_traverser(new Traverser(builder))
    , m_outEndpoint()
    , m_pool(threads)
    , m_mutex()
    , m_baseChunk()
    , m_sliceDepth(0)
    , m_wantedSchema(wantedSchema)
    , m_aboves()
    , m_tiles()
    , m_current()
    , m_processing()
{
    init(tileWidth);
}

Tiler::Tiler(
        const Builder& builder,
        const arbiter::Endpoint& outEndpoint,
        const std::size_t threads,
        const double tileWidth)
    : m_builder(builder)
    , m_traverser(new Traverser(builder))
    , m_outEndpoint(new arbiter::Endpoint(outEndpoint))
    , m_pool(threads)
    , m_mutex()
    , m_baseChunk()
    , m_sliceDepth(0)
    , m_wantedSchema(nullptr)
    , m_aboves()
    , m_tiles()
    , m_current()
    , m_processing()
{
    init(tileWidth);
}

void Tiler::init(const double tileWidth)
{
    if (!activeSchema().contains("X") || !activeSchema().contains("Y"))
    {
        throw std::runtime_error("Schema must contain X and Y");
    }

    const double fullWidth(m_builder.bbox().width());
    const Structure& structure(m_builder.structure());

    m_sliceDepth = m_builder.structure().coldDepthBegin();
    double div(structure.numChunksAtDepth(m_sliceDepth) / 2.0);

    std::cout << "Full width: " << fullWidth << std::endl;
    std::cout << "Max width:  " << tileWidth  << std::endl;

    while (
            fullWidth / div > tileWidth &&
            m_sliceDepth < structure.sparseDepthBegin())
    {
        std::cout << fullWidth / div << " : " << tileWidth << std::endl;
        div = structure.numChunksAtDepth(++m_sliceDepth) / 2.0;
    }

    std::cout << "Slice depth: " << m_sliceDepth << std::endl;
    std::cout << "Number of tiles: " << (div * 2) << std::endl;
    std::cout << "Tile width: " << fullWidth / static_cast<double>(div) <<
        std::endl;

    if (structure.hasBase()) m_aboves[m_builder.bbox()].reset(new Base(*this));
}

void Tiler::go(const TileFunction& f)
{
    std::size_t i(0);

    m_traverser->go([this, &f, &i](
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox,
            bool exists)->bool
    {
        if (depth < m_sliceDepth)
        {
            if (exists) insertAbove(f, chunkId, depth, bbox);
            return true;    // We always need to get to at least m_sliceDepth.
        }
        else if (depth == m_sliceDepth)
        {
            spawnTile(f, chunkId, bbox, exists);
            return exists;
        }
        else
        {
            if (exists) buildTile(f, chunkId, depth, bbox);
            return exists;
        }
    });

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_current.reset();
    }

    m_pool.join();

    if (!m_outEndpoint) return;

    const auto props(m_builder.propsToSave());
    for (const auto& p : props)
    {
        m_outEndpoint->putSubpath(p.first, p.second);
    }

    const std::string inHierPath(
            m_builder.outEndpoint().getSubEndpoint("h").type() + "://" +
            m_builder.outEndpoint().getSubEndpoint("h").root() + "*");

    if (!m_outEndpoint->isRemote())
    {
        arbiter::fs::mkdirp(m_outEndpoint->root() + "h");
    }

    const std::string outHierPath(m_outEndpoint->getSubEndpoint("h").root());

    m_builder.arbiter().copy(inHierPath, outHierPath);
}

void Tiler::insertAbove(
        const TileFunction& f,
        const Id& chunkId,
        const std::size_t depth,
        const BBox& bbox)
{
    Above& above(([this, &chunkId, depth, &bbox]()->Above&
    {
        std::size_t delta(m_sliceDepth - depth);
        const Schema& s(activeSchema());

        std::lock_guard<std::mutex> lock(m_mutex);
        auto result(
                m_aboves.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(bbox),
                    std::forward_as_tuple(new Above(chunkId, bbox, s, delta))));
        return *result.first->second;
    })());

    m_pool.add([this, &f, chunkId, &above]()
    {
        auto data(acquire(chunkId));

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            above.populate(std::move(data));
        }

        maybeProcess(f);
    });
}

void Tiler::spawnTile(
        const TileFunction& f,
        const Id& chunkId,
        const BBox& bbox,
        const bool exists)
{
    Tile& tile(([this, &chunkId, &bbox]()->Tile&
    {
        const Schema& s(activeSchema());

        std::lock_guard<std::mutex> lock(m_mutex);
        m_current.reset(new BBox(bbox));

        auto result(
                m_tiles.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(bbox),
                    std::forward_as_tuple(new Tile(bbox, s, m_aboves))));
        return *result.first->second;
    })());

    if (exists) awaitAndAcquire(f, chunkId, tile);
}

void Tiler::buildTile(
        const TileFunction& f,
        const Id& chunkId,
        const std::size_t depth,
        const BBox& bbox)
{
    Tile& tile(([this, &bbox]()->Tile&
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        // TODO Use asserts.
        if (!m_current) throw std::runtime_error("No current");
        if (!m_current->contains(bbox))
        {
            throw std::runtime_error("No encapsulating base");
        }

        return *m_tiles.at(*m_current);
    })());

    awaitAndAcquire(f, chunkId, tile);
}

void Tiler::awaitAndAcquire(
        const TileFunction& f,
        const Id& chunkId,
        Tile& tile)
{
    tile.await(chunkId);

    m_pool.add([this, &f, chunkId, &tile]()
    {
        auto data(acquire(chunkId));

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            tile.insert(chunkId, std::move(data));
        }

        maybeProcess(f);
    });
}

void Tiler::maybeProcess(const TileFunction& f)
{
    using QueueMap = std::map<BBox, TileMap::iterator>;
    QueueMap queue;

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        auto it(m_tiles.begin());
        while (it != m_tiles.end())
        {
            const BBox& tileBounds(it->first);
            std::unique_ptr<Tile>& tile(it->second);

            if ((!m_current || *m_current != tileBounds) && tile->acquire())
            {
                queue[tileBounds] = it;
            }

            ++it;
        }
    }

    for (auto& v : queue)
    {
        TileMap::iterator& it(v.second);
        Tile& tile(*it->second);
        tile.process(f);
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto& v : queue) m_tiles.erase(v.second);

        auto it(m_aboves.begin());

        // Predicate to ensure that no outstanding tiles overlap this Above.
        auto references([&it](const TileMap::value_type& v)
        {
            return v.second->references(*it->second);
        });

        // Predicate to ensure that we only remove Aboves that have had a child
        // Tile erased this iteration.
        auto isContainedBy([&it](const QueueMap::value_type& v)
        {
            return it->first.contains(v.first);
        });

        while (it != m_aboves.end())
        {
            if (
                    std::none_of(m_tiles.begin(), m_tiles.end(), references) &&
                    std::any_of(queue.begin(), queue.end(), isContainedBy))
            {
                it = m_aboves.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}

std::unique_ptr<std::vector<char>> Tiler::acquire(const Id& chunkId)
{
    const std::string path(m_builder.structure().maybePrefix(chunkId));
    auto compressed(Storage::ensureGet(m_builder.outEndpoint(), path));

    if (!compressed)
    {
        throw std::runtime_error("Could not acquire " + chunkId.str());
    }

    const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);
    auto data(
            Compression::decompress(
                *compressed,
                m_builder.schema(),
                m_wantedSchema,
                numPoints));

    return data;
}

Above::Set Tile::getContainingFrom(const BBox& bbox, Above::Map& aboves) const
{
    return std::accumulate(
            aboves.begin(),
            aboves.end(),
            Above::Set(),
            [&bbox](const Above::Set& in, Above::MapVal& a)
            {
                if (a.first.contains(bbox))
                {
                    auto out(in);
                    out.insert(a.second.get());
                    return out;
                }
                else return in;
            });
}

void Tile::process(const TileFunction& f)
{
    for (const Above* a : m_aboves)
    {
        if (const std::vector<char>* segment = a->data(m_bbox))
        {
            m_data.insert(m_data.end(), segment->begin(), segment->end());
        }
    }

    if (m_data.size())
    {
        VectorPointTable table(m_schema, m_data);
        SizedPointView view(table);
        f(view, m_bbox);
    }
}

} // namespace entwine

