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
#include <string>

#include <entwine/builder/chunk-cache.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/types/endpoints.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/source.hpp>
#include <entwine/types/threads.hpp>

namespace entwine
{

struct Builder
{
    Builder(
        Endpoints endpoints,
        Metadata metadata,
        Manifest manifest,
        Hierarchy hierarchy = Hierarchy());

    void run(
        Threads threads,
        uint64_t limit = 0,
        uint64_t progressIntervalSeconds = 10);

    void monitor(
        uint64_t progressIntervalSeconds,
        std::atomic_uint64_t& counter,
        std::atomic_bool& done);

    void runInserts(
        Threads threads,
        uint64_t limit,
        std::atomic_uint64_t& counter);
    void tryInsert(
        ChunkCache& cache,
        uint64_t origin,
        std::atomic_uint64_t& counter);
    void insert(
        ChunkCache& cache,
        uint64_t origin,
        std::atomic_uint64_t& counter);
    void save(unsigned threads);

    void saveHierarchy(unsigned threads);
    void saveSources(unsigned threads);
    void saveMetadata();

    Endpoints endpoints;
    Metadata metadata;
    Manifest manifest;
    Hierarchy hierarchy;
};

namespace builder
{

Builder load(Endpoints endpoints, unsigned threads, unsigned subsetId);
void merge(Builder& dst, const Builder& src, ChunkCache& cache);

} // namespace builder

} // namespace entwine
