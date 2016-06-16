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

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_set>
#include <unordered_map>

#include <entwine/third/json/json.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/spin-lock.hpp>
#include <entwine/util/unique.hpp>

#include <entwine/tree/splitter.hpp>


namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Builder;
class Cell;
class Climber;
class Clipper;
class Pool;

struct CountedChunk
{
    std::unique_ptr<Chunk> chunk;
    std::unordered_map<std::size_t, std::size_t> refs;

    void unref(std::size_t id)
    {
        if (!--refs.at(id))
        {
            assert(chunk);
            refs.erase(id);
            if (refs.empty()) chunk.reset();
        }
    }
};

class Cold : Splitter<CountedChunk>
{
    using SlotType = Splitter<CountedChunk>::Slot;

public:
    Cold(const Builder& builder, bool exists);
    ~Cold();

    Tube::Insertion insert(
            const Climber& climber,
            Clipper& clipper,
            Cell::PooledNode& cell);

    void save(const arbiter::Endpoint& endpoint) const;
    void clip(const Id& chunkId, std::size_t chunkNum, std::size_t id);

    void merge(const Cold& other);

    std::size_t clipThreads() const;

private:
    void ensureChunk(
            const Climber& climber,
            std::unique_ptr<Chunk>& chunk,
            bool exists);

    using ChunkMap = std::unordered_map<Id, std::unique_ptr<CountedChunk>>;

    const Builder& m_builder;
    Pool& m_pool;
};

} // namespace entwine

