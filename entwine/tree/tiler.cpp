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

#include <entwine/tree/chunk.hpp>
#include <entwine/tree/traverser.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

namespace
{

std::set<Id> fetchIds(const arbiter::Endpoint& ep)
{
    std::set<Id> s;

    const Json::Value json(parse(ep.get("entwine-ids")));
    for (Json::ArrayIndex i(0); i < json.size(); ++i)
    {
        s.emplace(json[i].asString());
    }

    return s;
}

} // unnamed namespace

void Above::populate(std::unique_ptr<std::vector<char>> data)
{
    const std::size_t pointSize(m_schema.pointSize());
    const std::size_t numPoints(data->size() / pointSize);

    VectorPointTable table(m_schema, *data);
    pdal::PointRef pointRef(table, 0);

    Point p(0, 0, m_bounds.mid().z);
    Bounds b;

    char* pos(data->data());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        pointRef.setPointId(i);

        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);

        // It's likely that the points will arrive in an order such that
        // many points in a row will belong to the same sub-bounds.
        if (!b.contains(p))
        {
            b = m_bounds;

            for (std::size_t d(0); d < m_delta; ++d)
            {
                b.go(getDirection(p, b.mid()), true);
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

    Point p(0, 0, m_bounds.mid().z);
    Bounds b;
    ChunkState s(m_metadata);

    char* pos(data->data());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        pointRef.setPointId(i);

        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);

        // It's likely that the points will arrive in an order such that
        // many points in a row will belong to the same sub-bounds.
        if (!b.contains(p))
        {
            s.reset();
            s.climbTo(p, m_delta);  // For the Base, delta = sliceDepth.
            b = s.chunkBounds();
        }

        auto& segment(m_segments[b]);
        segment.insert(segment.end(), pos, pos + pointSize);
    }

    m_here = true;
}

Base::Base(const Tiler& tiler)
    : Above(
            tiler.metadata().structure().baseIndexBegin(),
            tiler.metadata().boundsScaledCubic(),
            tiler.activeSchema(),
            tiler.sliceDepth())
    , m_metadata(tiler.metadata())
    , m_structure(tiler.metadata().structure())
{
    const Metadata& metadata(tiler.metadata());
    const Schema celledSchema(Schema::makeCelled(metadata.schema()));

    const std::string path(metadata.structure().maybePrefix(m_chunkId));
    auto data(Storage::ensureGet(tiler.inEndpoint(), path));

    if (!data)
    {
        throw std::runtime_error("Could not acquire base: " + m_chunkId.str());
    }

    /*
    std::unique_ptr<Schema> celledWantedSchema;

    if (const Schema* wantedSchema = tiler.wantedSchema())
    {
        celledWantedSchema.reset(
                new Schema(
                    BaseChunk::makeCelled(*wantedSchema)));
    }
    */

    // TODO.
    /*
    auto unpacker(tiler.metadata().format().unpack(std::move(data)));
    data = unpacker.acquireRawBytes();
    const std::size_t numPoints(unpacker.numPoints());

    std::cout << "Base points: " << numPoints << std::endl;
    const auto compression(tiler.metadata().format().compression());

    if (compression == ChunkCompression::LasZip)
    {
        throw std::runtime_error("No LasZip in Tiler's Base");
    }
    else if (compression == ChunkCompression::LazPerf)
    {
        data =
                Compression::decompress(
                    *data,
                    celledSchema,
                    // TODO We're tossing information by not using the celled
                    // version, so currently this is read-only - no
                    // transformations.
                    // celledWantedSchema.get(),
                    // tiler.wantedSchema(),
                    &tiler.activeSchema(),
                    numPoints);
    }

    populate(std::move(data));
    */
}

Tiler::Tiler(
        const arbiter::Endpoint& inEndpoint,
        const std::size_t threads,
        const double tileWidth,
        const Schema* wantedSchema,
        const std::size_t maxPointsPerTile)
    : m_inEndpoint(inEndpoint)
    , m_metadata(m_inEndpoint)
    , m_ids(fetchIds(m_inEndpoint))
    , m_maxPointsPerTile(maxPointsPerTile)
    , m_traverser(new Traverser(m_metadata, m_ids))
    , m_pool(threads)
    , m_mutex()
    , m_sliceDepth(0)
    , m_wantedSchema(wantedSchema)
    , m_aboves()
    , m_tiles()
    , m_current()
    , m_processing()
{
    init(tileWidth);
}

Tiler::~Tiler() { }

void Tiler::init(const double tileWidth)
{
    if (!activeSchema().contains("X") || !activeSchema().contains("Y"))
    {
        throw std::runtime_error("Schema must contain X and Y");
    }

    const double fullWidth(m_metadata.boundsScaledCubic().width());
    const Structure& structure(m_metadata.structure());

    m_sliceDepth = structure.coldDepthBegin();
    double div(structure.numChunksAtDepth(m_sliceDepth) / 2.0);

    std::cout << "Full bounds width: " << fullWidth << std::endl;
    std::cout << "Max tile width requested:  " << tileWidth  << std::endl;

    while (
            fullWidth / div > tileWidth &&
            m_sliceDepth < structure.sparseDepthBegin())
    {
        std::cout << fullWidth / div << " : " << tileWidth << std::endl;
        div = structure.numChunksAtDepth(++m_sliceDepth) / 2.0;
    }

    std::cout << "Slice depth: " << m_sliceDepth << std::endl;
    std::cout << "Nominal number of tiles: " << (div * 2) << std::endl;
    std::cout << "Actual tile width to use: " <<
        fullWidth / static_cast<double>(div) << std::endl;

    if (structure.hasBase())
    {
        m_aboves[m_metadata.boundsScaledCubic()].reset(new Base(*this));
    }
}

const Schema& Tiler::activeSchema() const
{
    return m_wantedSchema ? *m_wantedSchema : m_metadata.schema();
}

void Tiler::go(const TileFunction& f, const arbiter::Endpoint* ep)
{
    std::size_t i(0);

    m_traverser->go([this, &f, &i](const ChunkState& chunkState, bool exists)
    {
        const Id& chunkId(chunkState.chunkId());
        const std::size_t depth(chunkState.depth());
        const Bounds& bounds(chunkState.chunkBounds());

        if (depth < m_sliceDepth)
        {
            if (exists) insertAbove(f, chunkId, depth, bounds);
            return true;    // We always need to get to at least m_sliceDepth.
        }
        else if (depth == m_sliceDepth)
        {
            spawnTile(f, chunkId, bounds, exists);
            return exists;
        }
        else
        {
            if (exists) buildTile(f, chunkId, depth, bounds);
            return exists;
        }
    });

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_current.reset();
    }

    m_pool.join();
    m_pool.go();

    if (!ep) return;

    // TODO
    /*
    const auto props(m_builder.propsToSave());
    for (const auto& p : props)
    {
        ep->putSubpath(p.first, p.second);
    }

    const std::string inHierPath(
            m_builder.outEndpoint().getSubEndpoint("h").type() + "://" +
            m_builder.outEndpoint().getSubEndpoint("h").root() + "*");

    if (!ep->isRemote())
    {
        arbiter::fs::mkdirp(ep->root() + "h");
    }

    const std::string outHierPath(ep->getSubEndpoint("h").root());

    m_builder.arbiter().copy(inHierPath, outHierPath);
    */
}

void Tiler::insertAbove(
        const TileFunction& f,
        const Id& chunkId,
        const std::size_t depth,
        const Bounds& bounds)
{
    Above& above(([this, &chunkId, depth, &bounds]()->Above&
    {
        std::size_t delta(m_sliceDepth - depth);
        const Schema& s(activeSchema());

        std::lock_guard<std::mutex> lock(m_mutex);
        auto result(
                m_aboves.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(bounds),
                    std::forward_as_tuple(
                        new Above(chunkId, bounds, s, delta))));
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
        const Bounds& bounds,
        const bool exists)
{
    Tile& tile(([this, &chunkId, &bounds]()->Tile&
    {
        const Schema& s(activeSchema());

        std::lock_guard<std::mutex> lock(m_mutex);
        m_current.reset(new Bounds(bounds));

        auto result(
                m_tiles.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(bounds),
                    std::forward_as_tuple(
                        new Tile(bounds, s, m_aboves, m_maxPointsPerTile))));
        return *result.first->second;
    })());

    if (exists) awaitAndAcquire(f, chunkId, tile);
}

void Tiler::buildTile(
        const TileFunction& f,
        const Id& chunkId,
        const std::size_t depth,
        const Bounds& bounds)
{
    Tile& tile(([this, &bounds]()->Tile&
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        // TODO Use asserts.
        if (!m_current) throw std::runtime_error("No current");
        if (!m_current->contains(bounds))
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
    using QueueMap = std::map<Bounds, TileMap::iterator>;
    QueueMap queue;

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        auto it(m_tiles.begin());
        while (it != m_tiles.end())
        {
            const Bounds& tileBounds(it->first);
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
    const std::string path(m_metadata.structure().maybePrefix(chunkId));
    auto compressed(Storage::ensureGet(m_inEndpoint, path));

    if (!compressed)
    {
        throw std::runtime_error("Could not acquire " + chunkId.str());
    }

    /*
    const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);
    auto data(
            Compression::decompress(
                *compressed,
                m_metadata.schema(),
                m_wantedSchema,
                numPoints));

    return data;
    */
    return std::unique_ptr<std::vector<char>>();
}

Above::Set Tile::getContainingFrom(
        const Bounds& bounds,
        Above::Map& aboves) const
{
    return std::accumulate(
            aboves.begin(),
            aboves.end(),
            Above::Set(),
            [&bounds](const Above::Set& in, Above::MapVal& a)
            {
                if (a.first.contains(bounds))
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
        if (const std::vector<char>* segment = a->data(m_bounds))
        {
            m_data.insert(m_data.end(), segment->begin(), segment->end());
        }
    }

    if (m_data.size()) splitAndCall(f, m_data, m_bounds);
}

void Tile::splitAndCall(
        const TileFunction& f,
        const std::vector<char>& data,
        const Bounds& bounds) const
{
    const std::size_t numPoints(data.size() / m_schema.pointSize());

    if (numPoints <= m_maxPointsPerTile)
    {
        VectorPointTable table(m_schema, data);
        SizedPointView view(table);
        f(view, bounds);
    }
    else
    {
        std::vector<std::vector<char>> split(dirHalfEnd());

        BinaryPointTable table(m_schema);
        pdal::PointRef pointRef(table, 0);

        const char* pos(data.data());
        Point p;
        Dir dir;

        for (std::size_t i(0); i < numPoints; ++i)
        {
            table.setPoint(pos);
            p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
            p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
            p.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);
            dir = getDirection(bounds.mid(), p);

            std::vector<char>& active(split[toIntegral(dir, true)]);
            active.insert(active.end(), pos, pos + m_schema.pointSize());

            pos += m_schema.pointSize();
        }

        for (std::size_t i(0); i < split.size(); ++i)
        {
            splitAndCall(f, split[i], bounds.get(toDir(i), true));
        }
    }
}

} // namespace entwine

