/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/cache.hpp>

#include <entwine/drivers/source.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

FetchInfo::FetchInfo(
        Source& source,
        const Schema& schema,
        std::size_t id,
        std::size_t numPoints)
    : source(source)
    , schema(schema)
    , id(id)
    , numPoints(numPoints)
{ }

Block::Block(
        Cache& cache,
        const std::string& readerPath,
        const FetchInfoSet& fetches)
    : m_cache(cache)
    , m_readerPath(readerPath)
    , m_chunkMap()
{
    for (const auto& fetch : fetches)
    {
        m_chunkMap.insert(std::make_pair(fetch.id, nullptr));
    }
}

Block::~Block()
{
    m_cache.release(*this);
}

void Block::set(const std::size_t id, const ChunkReader* chunkReader)
{
    m_chunkMap.at(id) = chunkReader;
}

ChunkState::ChunkState()
    : chunkReader()
    , inactiveIt()
    , refs(0)
    , mutex()
{ }

ChunkState::~ChunkState() { }





Cache::Cache(const std::size_t maxChunks, const std::size_t maxChunksPerQuery)
    : m_maxChunks(std::max<std::size_t>(maxChunks, 16))
    , m_maxChunksPerQuery(std::max<std::size_t>(maxChunksPerQuery, 4))
    , m_chunkManager()
    , m_inactiveList()
    , m_activeCount(0)
    , m_mutex()
    , m_cv()
{ }

std::unique_ptr<Block> Cache::acquire(
        const std::string& readerPath,
        const FetchInfoSet& fetches)
{
    std::unique_ptr<Block> block(reserve(readerPath, fetches));

    if (!populate(readerPath, fetches, *block))
    {
        throw std::runtime_error("Invalid remote index state: " + readerPath);
    }

    return block;
}

void Cache::release(const Block& block)
{
    bool notify(false);
    const std::string path(block.path());

    std::unique_lock<std::mutex> lock(m_mutex);

    LocalManager& localManager(m_chunkManager.at(path));

    for (const auto& c : block.chunkMap())
    {
        const std::size_t id(c.first);

        std::unique_ptr<ChunkState>& chunkState(localManager.at(id));

        if (chunkState)
        {
            if (!--chunkState->refs)
            {
                m_inactiveList.push_front(GlobalChunkInfo(path, id));

                chunkState->inactiveIt.reset(
                        new InactiveList::iterator(m_inactiveList.begin()));

                --m_activeCount;
                notify = true;
            }
        }
        else
        {
            localManager.erase(id);
            if (localManager.empty()) m_chunkManager.erase(path);
        }
    }

    if (notify)
    {
        lock.unlock();
        m_cv.notify_all();
    }
}

std::unique_ptr<Block> Cache::reserve(
        const std::string& readerPath,
        const FetchInfoSet& fetches)
{
    if (fetches.size() > m_maxChunksPerQuery)
    {
        throw QueryLimitExceeded();
    }

    if (m_activeCount > m_maxChunks)
    {
        throw std::runtime_error("Unexpected cache state");
    }

    // TODO Not fair.
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this, &fetches]()->bool
    {
        return m_activeCount + fetches.size() <= m_maxChunks;
    });

    std::cout << "Fetching " << fetches.size() << " chunks" << std::endl;

    while (m_activeCount + fetches.size() + m_inactiveList.size() > m_maxChunks)
    {
        const GlobalChunkInfo& toRemove(m_inactiveList.back());

        LocalManager& localManager(m_chunkManager.at(toRemove.path));
        localManager.erase(toRemove.id);

        if (localManager.empty()) m_chunkManager.erase(toRemove.path);

        m_inactiveList.pop_back();
    }

    // Make the Block responsible for these chunks now, so even if something
    // throws during the fetching, we won't hold inactive reservations.
    std::unique_ptr<Block> block(new Block(*this, readerPath, fetches));

    LocalManager& localManager(m_chunkManager[readerPath]);

    // Reserve these fetches:
    //      - Insert (sans actual data) into the GlobalManager if non-existent
    //      - Increment the reference count - may be zero if inactive or new
    //      - If already existed and inactive, remove from the inactive list
    for (const auto& f : fetches)
    {
        std::unique_ptr<ChunkState>& chunkState(localManager[f.id]);

        if (!chunkState)
        {
            chunkState.reset(new ChunkState());
            ++m_activeCount;
        }
        else if (chunkState->inactiveIt)
        {
            m_inactiveList.erase(*chunkState->inactiveIt);
            chunkState->inactiveIt.reset(nullptr);
            ++m_activeCount;
        }

        ++chunkState->refs;
    }

    return block;
}

bool Cache::populate(
        const std::string& readerPath,
        const FetchInfoSet& fetches,
        Block& block)
{
    std::mutex mutex;
    bool success(true);
    Pool pool(std::min<std::size_t>(8, fetches.size()));

    for (const auto& f : fetches)
    {
        pool.add([this, &readerPath, &f, &block, &mutex, &success]()->void
        {
            if (const ChunkReader* chunkReader = fetch(readerPath, f))
            {
                std::lock_guard<std::mutex> lock(mutex);
                block.set(f.id, chunkReader);
            }
            else
            {
                success = false;
            }
        });
    }

    pool.join();

    return success;
}

const ChunkReader* Cache::fetch(
        const std::string& readerPath,
        const FetchInfo& fetchInfo)
{
    std::unique_lock<std::mutex> globalLock(m_mutex);
    ChunkState& chunkState(*m_chunkManager.at(readerPath).at(fetchInfo.id));
    globalLock.unlock();

    std::lock_guard<std::mutex> lock(chunkState.mutex);

    if (!chunkState.chunkReader)
    {
        std::unique_ptr<std::vector<char>> rawData(
                new std::vector<char>(
                    fetchInfo.source.get(std::to_string(fetchInfo.id))));

        chunkState.chunkReader =
                ChunkReader::create(
                    fetchInfo.schema,
                    fetchInfo.id,
                    fetchInfo.numPoints,
                    std::move(rawData));
    }

    return chunkState.chunkReader.get();
}

} // namespace entwine

