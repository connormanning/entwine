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
#include <entwine/reader/reader.hpp>
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

bool FetchInfo::operator<(const FetchInfo& other) const
{
    const auto& lhsEp(reader.endpoint());
    const auto& rhsEp(other.reader.endpoint());
    return
        (lhsEp.root() < rhsEp.root()) ||
        (lhsEp.root() == rhsEp.root() && (id < other.id));
}

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





Cache::Cache(const std::size_t maxBytes)
    : m_maxBytes(std::max<std::size_t>(maxBytes, 1024 * 1024 * 256))
    , m_maxHierarchyBytes(m_maxBytes / 8)
{ }

std::unique_ptr<Block> Cache::acquire(
        const std::string& readerPath,
        const FetchInfoSet& fetches)
{
    std::unique_ptr<Block> block(reserve(readerPath, fetches));

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
                block->set(f.id, chunkReader);
            }
            else
            {
                success = false;
            }
        });
    }

    pool.join();

    if (!success)
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

    while (m_activeBytes > m_maxBytes && m_inactiveList.size())
    {
        const GlobalChunkInfo& toRemove(m_inactiveList.back());

        LocalManager& localManager(m_chunkManager.at(toRemove.path));
        m_activeBytes -= localManager.at(toRemove.id)->chunkReader->size();
        localManager.erase(toRemove.id);

        if (localManager.empty()) m_chunkManager.erase(toRemove.path);

        m_inactiveList.pop_back();
        notify = true;
    }

    if (notify)
    {
        /*
        std::cout <<
            "\tData: " << (m_activeBytes / 1024 / 1024) << "MB" <<
            "\tFetches: " << block.chunkMap().size() <<
            "\tChunks: " << m_inactiveList.size() << std::endl;
        */

        lock.unlock();
        m_cv.notify_all();
    }
}

std::unique_ptr<Block> Cache::reserve(
        const std::string& readerPath,
        const FetchInfoSet& fetches)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this, &fetches]()->bool
    {
        return m_activeBytes < m_maxBytes;
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
        }
        else if (chunkState->inactiveIt)
        {
            m_inactiveList.erase(*chunkState->inactiveIt);
            chunkState->inactiveIt.reset();
        }

        ++chunkState->refs;
    }

    return block;
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

        globalLock.lock();
        m_activeBytes += chunkState.chunkReader->size();
    }

    return chunkState.chunkReader.get();
}

void Cache::markHierarchy(
        const std::string& name,
        const Hierarchy::Slots& touched)
{
    if (touched.empty()) return;

    std::lock_guard<std::mutex> topLock(m_hierarchyMutex);

    HierarchyCache& selected(m_hierarchyCache[name]);
    std::lock_guard<std::mutex> selectedLock(selected.mutex);

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

            m_hierarchyBytes += s->t->size();
        }
    }

    while (m_hierarchyBytes > m_maxHierarchyBytes && order.size())
    {
        const Hierarchy::Slot* s(order.back());
        order.pop_back();

        SpinGuard spinLock(s->spinner);
        m_hierarchyBytes -= s->t->size();
        s->t.reset();
        slots.erase(s);
    }

    std::cout <<
        "\tHier: " << (m_hierarchyBytes / 1024 / 1024) << "MB" <<
        "\tFetches: " << touched.size() << std::endl;

    assert(order.size() == slots.size());
}

} // namespace entwine

