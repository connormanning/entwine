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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/storage.hpp>
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
        m_base.mark = true;
        m_base.t = makeUnique<CountedChunk>();

        if (!exists)
        {
            m_base.t->chunk = Chunk::create(
                    m_builder,
                    0,
                    m_structure.baseIndexBegin(),
                    m_structure.baseIndexSpan());
        }
        else
        {
            const std::string basePath(
                    m_structure.baseIndexBegin().str() + metadata.postfix());

            if (auto data = m_builder.outEndpoint().tryGetBinary(basePath))
            {
                m_base.t->chunk = Chunk::create(
                        m_builder,
                        0,
                        m_structure.baseIndexBegin(),
                        m_structure.baseIndexSpan(),
                        std::move(data));
            }
            else
            {
                throw std::runtime_error("No base data found");
            }
        }
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

        const bool exists(slot.mark);
        slot.mark = true;

        if (!countedChunk) countedChunk = makeUnique<CountedChunk>();

        auto& refs(countedChunk->refs);

        if (!refs.count(clipper.id())) refs[clipper.id()] = 1;
        else ++refs[clipper.id()];

        if (!countedChunk->chunk)
        {
            ensureChunk(climber, countedChunk->chunk, exists);
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
        if (exists)
        {
            const std::string path(
                    m_builder.metadata().structure().maybePrefix(chunkId) +
                    m_builder.metadata().postfix(true));

            auto data(Storage::ensureGet(m_builder.outEndpoint(), path));

            chunk =
                    Chunk::create(
                        m_builder,
                        climber.depth(),
                        chunkId,
                        climber.pointsPerChunk(),
                        std::move(data));
        }
        else
        {
            chunk =
                    Chunk::create(
                        m_builder,
                        climber.depth(),
                        chunkId,
                        climber.pointsPerChunk());
        }

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

    if (m_structure.baseIndexSpan())
    {
        dynamic_cast<BaseChunk&>(*m_base.t->chunk).save(endpoint);
    }

    Json::Value json;
    for (const auto& id : ids()) json.append(id.str());

    const std::string subpath("entwine-ids" + m_builder.metadata().postfix());
    Storage::ensurePut(endpoint, subpath, toFastString(json));
}

void Cold::clip(
        const Id& chunkId,
        const std::size_t chunkNum,
        const std::size_t id,
        const bool sync)
{
    auto& slot(at(chunkId, chunkNum));
    assert(slot.mark);

    auto unref([&slot, id]()
    {
        assert(slot.t);
        SpinGuard lock(slot.spinner);
        slot.t->unref(id);
    });

    if (!sync) m_pool.add(unref);
    else unref();
}

void Cold::merge(const Cold& other)
{
    if (m_base.t->chunk)
    {
        dynamic_cast<BaseChunk&>(*m_base.t->chunk).merge(
                dynamic_cast<BaseChunk&>(*other.m_base.t->chunk));
    }

    Splitter::merge(other.ids());
}

} // namespace entwine

