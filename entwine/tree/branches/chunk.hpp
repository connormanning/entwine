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
#include <vector>

#include <entwine/types/elastic-atomic.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

struct Entry
{
    Entry(std::atomic<const Point*>& point, char* pos, std::mutex& mutex);

    bool hasPoint() const;

    std::atomic<const Point*>& point;
    char* pos;
    std::mutex& mutex;
};

class Schema;

class Chunk
{
public:
    Chunk(const Schema& schema, std::size_t id, std::size_t numPoints);
    Chunk(const Schema& schema, std::size_t id, const std::vector<char>& data);
    ~Chunk();

    const std::vector<char>& data() const;
    std::unique_ptr<Entry> getEntry(std::size_t rawIndex);

private:
    std::size_t normalizeIndex(std::size_t rawIndex) const;
    char* getPosition(std::size_t index);

    const Schema& m_schema;
    const std::size_t m_id;
    const std::size_t m_numPoints;

    std::vector<ElasticAtomic<const Point*>> m_points;
    std::vector<char> m_data;
    std::vector<std::mutex> m_locks;
};

} // namespace entwine

