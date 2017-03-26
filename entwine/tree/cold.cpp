/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/cold.hpp>

#include <chrono>
#include <thread>

#include <entwine/formats/cesium/tileset.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const std::size_t maxCreateTries(8);
    const auto createSleepTime(std::chrono::milliseconds(500));

    const std::size_t maxFastTrackers(std::pow(4, 12));
}

Cold::Cold(const Builder& builder, bool exists)
    : Splitter(builder.metadata().structure())
    , m_builder(builder)
    , m_pool(m_builder.threadPools().clipPool())
{
    const Metadata& metadata(m_builder.metadata());

    if (exists)
    {
        const Json::Value json(([this, &metadata]()
        {
            const std::string subpath("entwine-ids" + metadata.postfix());
            return parse(m_builder.outEndpoint().get(subpath));
        })());

        Id chunkId(0);

        for (Json::ArrayIndex i(0); i < json.size(); ++i)
        {
            chunkId = Id(json[i].asString());

            const ChunkInfo chunkInfo(m_structure.getInfo(chunkId));
            const std::size_t chunkNum(chunkInfo.chunkNum());

            mark(chunkId, chunkNum);
        }
    }

    if (m_structure.baseIndexSpan())
    {
        m_base.exists = true;
        m_base.t = makeUnique<CountedChunk>();
        m_base.t->chunk = Chunk::create(
                m_builder,
                metadata.boundsScaledCubic(),
                0,
                m_structure.baseIndexBegin(),
                m_structure.baseIndexSpan(),
                exists);
    }
}

Cold::~Cold()
{ }

Tube::Insertion Cold::insert(
        const Climber& climber,
        Clipper& clipper,
        Cell::PooledNode& cell)
{
    if (isWithinBase(climber.depth()))
    {
        return m_base.t->chunk->insert(climber, cell);
    }

    auto& slot(getOrCreate(climber.chunkId(), climber.chunkNum()));
    std::unique_ptr<CountedChunk>& countedChunk(slot.t);

    // With this insertion check into our single-threaded Clipper (which we
    // need to perform anyways), we can avoid locking this Chunk to check for
    // existence.
    if (clipper.insert(climber.chunkId(), climber.chunkNum(), climber.depth()))
    {
        UniqueSpin slotLock(slot.spinner);

        const bool alreadyExists(slot.exists);
        slot.exists = true;

        if (!countedChunk) countedChunk = makeUnique<CountedChunk>();

        auto& refs(countedChunk->refs);

        if (!refs.count(clipper.id())) refs[clipper.id()] = 1;
        else ++refs[clipper.id()];

        if (!countedChunk->chunk)
        {
            ensureChunk(climber, countedChunk->chunk, alreadyExists);
        }
    }

    return countedChunk->chunk->insert(climber, cell);
}

void Cold::ensureChunk(
        const Climber& climber,
        std::unique_ptr<Chunk>& chunk,
        const bool exists)
{
    const Id& chunkId(climber.chunkId());

    std::size_t tries(0);
    while (!chunk)
    {
        chunk = Chunk::create(
                m_builder,
                climber.chunkBounds(),
                climber.depth(),
                chunkId,
                climber.pointsPerChunk(),
                exists);

        if (!chunk)
        {
            if (++tries < maxCreateTries)
            {
                std::cout <<
                    "Failed chunk create on " <<
                    (exists ? "existing" : "new") << " chunk: " << chunkId <<
                    std::endl;
                std::this_thread::sleep_for(createSleepTime);
            }
            else
            {
                std::cout << "Invalid chunk at " << chunkId << std::endl;
                std::cout << "Non-recoverable error - exiting" << std::endl;
                exit(1);
            }
        }
    }
}

void Cold::save(const arbiter::Endpoint& endpoint) const
{
    m_pool.join();

    if (BaseChunk* baseChunk = dynamic_cast<BaseChunk*>(m_base.t->chunk.get()))
    {
        baseChunk->save();
    }

    const auto aggregated(ids());
    Json::Value json;
    for (const auto& id : aggregated) json.append(id.str());

    const std::string subpath("entwine-ids" + m_builder.metadata().postfix());
    io::ensurePut(endpoint, subpath, toFastString(json));

    if (m_builder.metadata().cesiumSettings()) saveCesiumMetadata(endpoint);
}

void Cold::saveCesiumMetadata(const arbiter::Endpoint& endpoint) const
{
    BaseChunk* baseChunk(dynamic_cast<BaseChunk*>(m_base.t->chunk.get()));
    if (!baseChunk)
    {
        throw std::runtime_error("Cesium output requires a base span");
    }

    std::cout << "Treeifying" << std::endl;
    std::map<Id, cesium::TileInfo> tiles(m_info);

    const auto infoList(baseChunk->baseInfo());
    for (std::size_t i(0); i < infoList.size(); ++i)
    {
        auto& tile(tiles[infoList[i].id()]);
        tile = infoList[i];
        tile.visit();

        if (i + 1 < infoList.size())
        {
            auto& child(tiles[infoList[i + 1].id()]);
            child = infoList[i + 1];
            tile.addChild(child);
        }
    }

    auto treeify([&](cesium::TileInfo& leaf)
    {
        if (leaf.id() == m_structure.baseIndexBegin()) return;

        cesium::TileInfo* info(&leaf);
        Id parentId(
                info->depth() > m_structure.coldDepthBegin() ?
                    ChunkInfo::calcParentId(
                        m_structure,
                        info->id(),
                        info->depth()) :
                    ChunkInfo::calcLevelIndex(
                        m_structure.dimensions(),
                        info->depth() - 1));

        while (!tiles.at(parentId).addChild(*info))
        {
            info = &tiles.at(parentId);
            parentId =
                    info->depth() > m_structure.coldDepthBegin() ?
                        ChunkInfo::calcParentId(
                            m_structure,
                            info->id(),
                            info->depth()) :
                        ChunkInfo::calcLevelIndex(
                                m_structure.dimensions(),
                                info->depth() - 1);
        }
    });

    for (auto it(tiles.rbegin()); it != tiles.rend(); ++it) treeify(it->second);

    std::cout << "Serializing tileset metadata" << std::endl;

    cesium::Tileset tileset(
            m_builder.metadata(),
            tiles.at(m_structure.baseIndexBegin()));
    tileset.writeTo(endpoint.getSubEndpoint("cesium"));

    std::cout << "Tileset written" << std::endl;
}

void Cold::clip(
        const Id& chunkId,
        const std::size_t chunkNum,
        const std::size_t id,
        const bool sync)
{
    auto& slot(at(chunkId, chunkNum));
    assert(slot.exists);

    auto unref([this, chunkId, &slot, id]()
    {
        SpinGuard lock(slot.spinner);
        assert(slot.t);

        if (m_builder.metadata().cesiumSettings() && slot.t->unique())
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_info[chunkId] = slot.t->chunk->info();
        }

        slot.t->unref(id);
    });

    if (!sync) m_pool.add(unref);
    else unref();
}

void Cold::merge(const Cold& other)
{
    if (m_base.t->chunk)
    {
        Splitter::merge(
                dynamic_cast<BaseChunk&>(*m_base.t->chunk).merge(
                    dynamic_cast<BaseChunk&>(*other.m_base.t->chunk)));
    }

    Splitter::merge(other.ids());
}

} // namespace entwine

