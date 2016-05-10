/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/traverser.hpp>

#include <algorithm>
#include <numeric>

#include <pdal/PointRef.hpp>

#include <entwine/tree/chunk.hpp>
#include <entwine/tree/registry.hpp>

namespace entwine
{

Traverser::Traverser(const Builder& builder, const std::set<Id>* ids)
    : m_builder(builder)
    , m_structure(m_builder.structure())
    , m_ids(ids ? *ids : m_builder.registry().ids())
{ }

Tiler::Tiler(
        const Builder& builder,
        const std::size_t threads,
        const double maxArea,
        const Schema* wantedSchema)
    : m_builder(builder)
    , m_traverser(builder)
    , m_outEndpoint()
    , m_pool(threads)
    , m_mutex()
    , m_baseChunk()
    , m_current()
    , m_above()
    , m_tiles()
    , m_sliceDepth(0)
    , m_processing()
    , m_wantedSchema(wantedSchema)
{
    init(maxArea);
}

Tiler::Tiler(
        const Builder& builder,
        const arbiter::Endpoint& outEndpoint,
        const std::size_t threads,
        const double maxArea)
    : m_builder(builder)
    , m_traverser(builder)
    , m_outEndpoint(new arbiter::Endpoint(outEndpoint))
    , m_pool(threads)
    , m_mutex()
    , m_baseChunk()
    , m_current()
    , m_above()
    , m_tiles()
    , m_sliceDepth(0)
    , m_processing()
    , m_wantedSchema(nullptr)
{
    init(maxArea);
}

void Tiler::init(const double maxArea)
{
    const double fullWidth(m_builder.bbox().width());
    const double maxWidth(std::sqrt(maxArea));

    const Structure& structure(m_builder.structure());

    m_sliceDepth = m_builder.structure().coldDepthBegin();
    double div(structure.numChunksAtDepth(m_sliceDepth) / 2.0);

    std::cout << "Full width: " << fullWidth << std::endl;
    std::cout << "Max width:  " << maxWidth  << std::endl;

    while (
            fullWidth / div > maxWidth &&
            m_sliceDepth < structure.sparseDepthBegin())
    {
        std::cout << fullWidth / div << " : " << maxWidth << std::endl;
        div = structure.numChunksAtDepth(++m_sliceDepth) / 2.0;
    }

    std::cout << "Slice depth: " << m_sliceDepth << std::endl;
    std::cout << "Number of tiles: " << (div * 2) << std::endl;
    std::cout << "Tile width: " << fullWidth / static_cast<double>(div) <<
        std::endl;

    if (structure.hasBase())
    {
        const Structure& structure(m_builder.structure());

        const Id& baseId(structure.baseIndexBegin());
        const Schema celledSchema(BaseChunk::makeCelled(m_builder.schema()));
        auto compressed(acquire(baseId));

        if (!compressed)
        {
            throw std::runtime_error("Could not acquire base: " + baseId.str());
        }

        const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);

        m_baseChunk =
            Compression::decompress(*compressed, celledSchema, numPoints);
    }
}

void Tiler::go(const TileFunction& f)
{
    std::size_t i(0);

    m_traverser.go([this, &f, &i](
            const Id& chunkId,
            std::size_t depth,
            const BBox& bbox,
            bool exists)->bool
    {
        // TODO Should call this even if !exists to make sure that f() gets
        // called even for areas whose depth doesn't reach the cold depth.
        if (exists) insert(f, chunkId, depth, bbox, exists);
        return exists;
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

    if (m_baseChunk)
    {
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
    }

    if (m_tiles.size()) std::cout << " Left: " << m_tiles.size() << std::endl;

    for (const auto& p : m_above)
    {
        std::cout << "A: " << p.first << std::endl;
        for (const auto& s : p.second)
        {
            std::cout << "\t" << s.first << std::endl;
        }
    }

    for (const auto& p : m_tiles)
    {
        std::cout << "O: " << p.first << std::endl;
        for (const auto& s : p.second)
        {
            std::cout << "\t" << s.first << std::endl;
        }
    }
}

void Tiler::insert(
        const TileFunction& f,
        const Id& chunkId,
        const std::size_t depth,
        const BBox& bbox,
        const bool exists)
{
    BBox base;
    std::unique_ptr<std::vector<char>>* handlePtr(nullptr);

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        if (depth < m_sliceDepth)
        {
            handlePtr = &m_above[bbox][chunkId];
            base = bbox;
        }
        else if (depth == m_sliceDepth)
        {
            m_current.reset(new BBox(bbox));
            base = bbox;

            m_tiles[bbox][chunkId] = nullptr;
            handlePtr = &m_tiles[bbox][chunkId];
        }
        else
        {
            auto pred([&bbox](const Tiles::value_type& p)
            {
                return p.first.contains(bbox, true);
            });

            auto baseIt(std::find_if(m_tiles.begin(), m_tiles.end(), pred));
            if (baseIt != m_tiles.end())
            {
                base = baseIt->first;

                m_tiles[base][chunkId] = nullptr;
                handlePtr = &m_tiles[base][chunkId];
            }
            else
            {
                throw std::runtime_error(
                        "Couldn't find encapsulating base for " +
                        chunkId.str());
            }
        }
    }

    if (!handlePtr) throw std::runtime_error("Invalid chunk handle");
    std::unique_ptr<std::vector<char>>& handle(*handlePtr);

    m_pool.add([this, &f, chunkId, depth, &handle]()
    {
        auto compressed(acquire(chunkId));

        if (!compressed)
        {
            throw std::runtime_error("Could not acquire " + chunkId.str());
        }

        const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);

        auto data(Compression::decompress(
                *compressed,
                m_builder.schema(),
                numPoints));

        compressed.reset();

        std::unique_lock<std::mutex> lock(m_mutex);
        handle = std::move(data);

        std::vector<BBox> maybes;
        for (const auto& t : m_tiles)
        {
            if (!m_current || *m_current != t.first)
            {
                maybes.push_back(t.first);
            }
        }

        lock.unlock();

        for (const auto& b : maybes)
        {
            maybeProcess(f, m_sliceDepth, b);
        }
    });
}

void Tiler::maybeProcess(
        const TileFunction& f,
        const std::size_t depth,
        const BBox& base)
{
    if (depth < m_sliceDepth)
    {
        explodeAbove(f, base);
        return;
    }

    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_processing.count(base) && allHere(base))
    {
        const auto& tile(m_tiles.at(base));
        m_processing.insert(base);

        lock.unlock();

        actuallyProcess(f, base);

        const Schema& schema(m_builder.schema());

        if (m_outEndpoint)
        {
            for (const auto& t : tile)
            {
                if (!t.second)
                {
                    std::cout << "BAD BASE " << base << std::endl;
                    for (auto& c : tile)
                    {
                        std::cout << "\tN: " << c.first << std::endl;
                    }
                    throw std::runtime_error(
                            "Invalid tile data: " + t.first.str());
                }

                auto toWrite(Compression::compress(*t.second, schema));

                Chunk::pushTail(
                        *toWrite,
                        Chunk::Tail(
                            t.second->size() / schema.pointSize(),
                            Chunk::Contiguous)); // TODO Get from ChunkInfo.

                m_outEndpoint->putSubpath(t.first.str(), *toWrite);
            }
        }

        lock.lock();
        m_tiles.erase(base);
        m_processing.erase(base);

        auto it(m_above.begin());

        auto overlaps([&it](const Tiles::value_type& p)
        {
            return it->first.overlaps(p.first);
        });

        while (it != m_above.end())
        {
            if (std::none_of(m_tiles.begin(), m_tiles.end(), overlaps))
            {
                const ChunkData& chunkData(it->second);
                const Id& id(chunkData.begin()->first);

                if (chunkData.size() != 1)
                {
                    throw std::runtime_error("Unexpected supertile contents.");
                }

                if (m_outEndpoint)
                {
                    const auto& data(*chunkData.begin()->second);

                    auto toWrite(Compression::compress(data, schema));

                    Chunk::pushTail(
                            *toWrite,
                            Chunk::Tail(
                                data.size() / schema.pointSize(),
                                Chunk::Contiguous)); // TODO Get from ChunkInfo.

                    m_outEndpoint->putSubpath(id.str(), *toWrite);
                }

                // std::cout << "Erasing above: " << id << std::endl;
                it = m_above.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}

bool Tiler::allHere(const BBox& base) const
{
    if (m_current && base == *m_current) return false;
    if (!m_tiles.count(base)) return false;

    // We have placeholders (and maybe the associated data) for all chunks
    // needed for our resident tile.  Check if all data has been fetched, and
    // process the tile if so.
    const auto& tile(m_tiles.at(base));

    auto exists([](const ChunkData::value_type& p)
    {
        return p.second.get() != nullptr;
    });

    auto overlapExists([&base](const Tiles::value_type& t)
    {
        if (!t.first.contains(base)) return true;
        else if (t.second.size() == 1)
        {
            return t.second.begin()->second.get() != nullptr;
        }
        else throw std::runtime_error("Invalid 'above' tile size");
    });

    return
        std::all_of(tile.begin(), tile.end(), exists) &&
        std::all_of(m_above.begin(), m_above.end(), overlapExists);
}

void Tiler::explodeAbove(const TileFunction& f, const BBox& superTile)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    const std::vector<BBox> subs(
            std::accumulate(
                m_tiles.begin(),
                m_tiles.end(),
                std::vector<BBox>(),
                [&superTile](
                        const std::vector<BBox>& subs,
                        const Tiles::value_type& t)
                {
                    if (superTile.contains(t.first))
                    {
                        std::vector<BBox> out(subs);
                        out.push_back(t.first);
                        return out;
                    }
                    else
                    {
                        return subs;
                    }
                }));

    lock.unlock();

    std::for_each(subs.begin(), subs.end(), [this, &f](const BBox& sub)
    {
        maybeProcess(f, m_sliceDepth, sub);
    });
}

void Tiler::actuallyProcess(const TileFunction& f, const BBox& base)
{
    TiledPointTable nativeTable(
            m_builder.schema(),
            base,
            m_tiles,
            m_above,
            m_baseChunk.get(),
            m_mutex);

    SizedPointView nativeView(nativeTable);

    if (m_outEndpoint || !m_wantedSchema)
    {
        f(nativeView, base);
    }
    else
    {
        // If this is a read-only call and we want a different schema,
        // provide it as a contiguous chunk.
        std::vector<char> data(
                nativeTable.size() * m_wantedSchema->pointSize());

        char* pos(data.data());

        for (std::size_t i(0); i < nativeTable.size(); ++i)
        {
            for (const auto& d : m_wantedSchema->dims())
            {
                nativeView.getField(pos, d.id(), d.type(), i);
                pos += d.size();
            }
        }

        VectorPointTable wantedTable(*m_wantedSchema, data);
        SizedPointView wantedView(wantedTable);
        f(wantedView, base);
    }
}

TiledPointTable::TiledPointTable(
        const Schema& schema,
        const BBox& bbox,
        Tiler::Tiles& tiles,
        Tiler::Tiles& above,
        std::vector<char>* baseChunk,
        std::mutex& mutex)
    : BasePointTable(schema.pdalLayout())
    , m_schema(schema)
    , m_points()
{
    if (baseChunk)
    {
        const Schema& celledSchema(BaseChunk::makeCelled(schema));

        VectorPointTable table(celledSchema, *baseChunk);
        pdal::PointRef pointRef(table, 0);

        Point p;

        for (std::size_t i(0); i < table.size(); ++i)
        {
            pointRef.setPointId(i);

            p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
            p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
            p.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

            if (bbox.contains(p))
            {
                // Skip tube IDs.
                m_points.push_back(table.getPoint(i) + sizeof(uint64_t));
            }
        }
    }

    std::lock_guard<std::mutex> lock(mutex);
    for (auto& t : above)
    {
        const BBox& aboveBounds(t.first);
        Tiler::ChunkData& chunkData(t.second);

        if (aboveBounds.contains(bbox))
        {
            for (auto& c : chunkData) addInRange(*c.second, bbox);
        }
    }

    Tiler::ChunkData& tile(tiles.at(bbox));
    for (auto& c : tile) addAll(*c.second);
}

void TiledPointTable::addInRange(std::vector<char>& chunk, const BBox& bbox)
{
    VectorPointTable table(m_schema, chunk);
    pdal::PointRef pointRef(table, 0);

    Point p;

    for (std::size_t i(0); i < table.size(); ++i)
    {
        pointRef.setPointId(i);

        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
        p.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

        if (bbox.contains(p)) m_points.push_back(table.getPoint(i));
    }
}

void TiledPointTable::addAll(std::vector<char>& chunk)
{
    char* pos(chunk.data());
    const char* end(pos + chunk.size());

    while (pos < end)
    {
        m_points.push_back(pos);
        pos += m_schema.pointSize();
    }
}

} // namespace entwine

