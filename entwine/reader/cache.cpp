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

#include <entwine/reader/chunk-reader.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

FetchInfo::FetchInfo(
        const Reader& reader,
        const Id& id,
        const Id& numPoints,
        const std::size_t depth)
    : reader(reader)
    , id(id)
    , numPoints(numPoints)
    , depth(depth)
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

void Block::set(const Id& id, const ChunkReader* chunkReader)
{
    m_chunkMap.at(id) = chunkReader;
}

DataChunkState::DataChunkState()
    : chunkReader()
    , inactiveIt()
    , refs(0)
    , mutex()
{ }

DataChunkState::~DataChunkState() { }





Cache::Cache(const std::size_t maxChunks)
    : m_maxChunks(std::max<std::size_t>(maxChunks, 16))
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
        const Id& id(c.first);

        std::unique_ptr<DataChunkState>& chunkState(localManager.at(id));

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
            std::cout << "Removing a bad fetch" << std::endl;
            localManager.erase(id);
            if (localManager.empty()) m_chunkManager.erase(path);
        }
    }

    std::cout <<
        "\tActive size: " << m_activeCount <<
        "\tIdle size: " << m_inactiveList.size() <<
        "\tThis query: " << block.chunkMap().size() << std::endl;

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
    if (m_activeCount > m_maxChunks)
    {
        throw std::runtime_error("Unexpected cache state");
    }

    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this, &fetches]()->bool
    {
        return m_activeCount + fetches.size() <= m_maxChunks;
    });

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
        std::unique_ptr<DataChunkState>& chunkState(localManager[f.id]);

        if (!chunkState)
        {
            chunkState.reset(new DataChunkState());
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

    // Do the removal after the reservation so we don't remove anything we are
    // about to need to fetch.
    while (m_activeCount + m_inactiveList.size() > m_maxChunks)
    {
        const GlobalChunkInfo& toRemove(m_inactiveList.back());

        LocalManager& localManager(m_chunkManager.at(toRemove.path));
        localManager.erase(toRemove.id);

        if (localManager.empty()) m_chunkManager.erase(toRemove.path);

        m_inactiveList.pop_back();
    }

    return block;
}

bool Cache::populate(
        const std::string& readerPath,
        const FetchInfoSet& fetches,
        Block& block)
{
    bool success(true);
    std::mutex mutex;
    Pool pool(std::min<std::size_t>(2, fetches.size()));

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
    DataChunkState& chunkState(*m_chunkManager.at(readerPath).at(fetchInfo.id));
    globalLock.unlock();

    std::lock_guard<std::mutex> lock(chunkState.mutex);

    if (!chunkState.chunkReader)
    {
        const Reader& reader(fetchInfo.reader);
        const Metadata& metadata(reader.metadata());
        const std::string path(metadata.structure().maybePrefix(fetchInfo.id));

        chunkState.chunkReader = makeUnique<ChunkReader>(
                metadata,
                fetchInfo.id,
                fetchInfo.depth,
                makeUnique<std::vector<char>>(
                    reader.endpoint().getBinary(path)));
    }

    return chunkState.chunkReader.get();
}

void Cache::markHierarchy(
        const std::string& name,
        const Hierarchy::Slots& touched)
{
    if (touched.empty()) return;

    std::cout << "Blocks touched: " << touched.size() << std::endl;

    std::unique_lock<std::mutex> topLock(m_hierarchyMutex);
    HierarchyCache& selected(m_hierarchyCache[name]);
    std::lock_guard<std::mutex> lock(selected.mutex);
    topLock.unlock();

    auto& slots(selected.slots);
    auto& order(selected.order);

    for (const Hierarchy::Slot* s : touched)
    {
        if (slots.count(s))
        {
            order.splice(order.begin(), order, slots.at(s));
        }
        else
        {
            auto it(slots.insert(std::make_pair(s, order.end())).first);
            order.push_front(s);
            it->second = order.begin();
        }
    }

    std::cout << "Awake for " << name << ": " << slots.size() << std::endl;

    if (slots.size() > m_maxChunks)
    {
        std::cout << "Erasing " << (slots.size() - m_maxChunks) << std::endl;
    }

    while (order.size() > m_maxChunks)
    {
        const Hierarchy::Slot* s(order.back());
        order.pop_back();

        SpinGuard spinLock(s->spinner);
        s->t.reset();
        slots.erase(s);
    }

    assert(order.size() == slots.size());
}

} // namespace entwine

