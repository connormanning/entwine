/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/builder.hpp>

#include <algorithm>
#include <cassert>

#include <pdal/PipelineManager.hpp>

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/heuristics.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/point-counts.hpp>
#include <entwine/util/config.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pdal-mutex.hpp>
#include <entwine/util/pipeline.hpp>
#include <entwine/util/time.hpp>

namespace entwine
{

Builder::Builder(
    Endpoints endpoints,
    Metadata metadata,
    Manifest manifest,
    Hierarchy hierarchy)
    : endpoints(endpoints)
    , metadata(metadata)
    , manifest(manifest)
    , hierarchy(hierarchy)
{ }

uint64_t Builder::run(
    const Threads threads,
    const uint64_t limit,
    const uint64_t progressInterval)
{
    Pool pool(2);

    std::atomic_uint64_t counter(0);
    std::atomic_bool done(false);
    pool.add([&]() { monitor(progressInterval, counter, done); });
    pool.add([&]() { runInserts(threads, limit, counter); done = true; });

    pool.join();

    return counter;
}

void Builder::runInserts(
    Threads threads,
    uint64_t limit,
    std::atomic_uint64_t& counter)
{
    const Bounds active = metadata.subset
        ? intersection(
            getBounds(metadata.bounds, *metadata.subset),
            metadata.boundsConforming
        )
        : metadata.boundsConforming;

    const uint64_t actualWorkThreads =
        std::min<uint64_t>(threads.work, manifest.size());
    const uint64_t stolenThreads = threads.work - actualWorkThreads;
    const uint64_t actualClipThreads = threads.clip + stolenThreads;

    ChunkCache cache(endpoints, metadata, hierarchy, actualClipThreads);
    Pool pool(std::min<uint64_t>(actualWorkThreads, manifest.size()));

    uint64_t filesInserted = 0;

    for (
        uint64_t origin = 0;
        origin < manifest.size() && (!limit || filesInserted < limit);
        ++origin)
    {
        const auto& item = manifest.at(origin);
        const auto& info = item.source.info;
        if (!item.inserted && info.points && active.overlaps(info.bounds))
        {
            std::cout << "Adding " << origin << " - " << item.source.path <<
                std::endl;

            pool.add([this, &cache, origin, &counter]()
            {
                tryInsert(cache, origin, counter);
                std::cout << "\tDone " << origin << std::endl;
            });

            ++filesInserted;
        }
    }

    std::cout << "Joining" << std::endl;

    pool.join();
    cache.join();

    save(getTotal(threads));
}

void Builder::monitor(
    const uint64_t progressInterval,
    std::atomic_uint64_t& atomicCurrent,
    std::atomic_bool& done)
{
    if (!progressInterval) return;

    using ms = std::chrono::milliseconds;
    const double mph = 3600.0 / 1000000.0;

    const double already = getInsertedPoints(manifest);
    const double total = getTotalPoints(manifest);
    const auto start = now();

    int64_t lastTick = 0;
    double lastInserted = 0;

    while (!done)
    {
        std::this_thread::sleep_for(ms(1000 - (since<ms>(start) % 1000)));
        const int64_t tick = since<std::chrono::seconds>(start);

        if (tick == lastTick || tick % progressInterval) continue;

        lastTick = tick;

        const double current = atomicCurrent;
        const double inserted = already + current;
        const double progress = inserted / total;

        const uint64_t pace = inserted / tick * mph;
        const uint64_t intervalPace =
            (inserted - lastInserted) / progressInterval * mph;

        lastInserted = inserted;

        std::cout << formatTime(tick) << " - " <<
            std::round(progress * 100) << "% - " <<
            commify(inserted) << " - " <<
            commify(pace) << " " <<
            "(" << commify(intervalPace) << ") M/h" <<
            std::endl;
    }
}

void Builder::tryInsert(
    ChunkCache& cache,
    const Origin originId,
    std::atomic_uint64_t& counter)
{
    auto& item = manifest.at(originId);

    try
    {
        insert(cache, originId, counter);
    }
    catch (const std::exception& e)
    {
        item.source.info.errors.push_back(e.what());
    }
    catch (...)
    {
        item.source.info.errors.push_back("Unknown error during build");
    }

    item.inserted = true;
}

void Builder::insert(
    ChunkCache& cache,
    const Origin originId,
    std::atomic_uint64_t& counter)
{
    auto& item = manifest.at(originId);
    auto& info(item.source.info);
    const auto handle =
        ensureGetLocalHandle(*endpoints.arbiter, item.source.path);

    const std::string localPath = handle.localPath();

    ChunkKey ck(metadata.bounds, getStartDepth(metadata));
    Clipper clipper(cache);

    optional<ScaleOffset> so = getScaleOffset(metadata.schema);
    const optional<Bounds> boundsSubset = metadata.subset
        ? getBounds(metadata.bounds, *metadata.subset)
        : optional<Bounds>();

    uint64_t inserted(0);
    uint64_t pointId(0);

    auto layout = toLayout(metadata.absoluteSchema);
    VectorPointTable table(layout);
    table.setProcess([&]()
    {
        inserted += table.numPoints();
        if (inserted > heuristics::sleepCount)
        {
            inserted = 0;
            clipper.clip();
        }

        Voxel voxel;
        PointCounts counts;

        Key key(metadata.bounds, getStartDepth(metadata));

        for (auto it = table.begin(); it != table.end(); ++it)
        {
            auto& pr = it.pointRef();
            pr.setField(DimId::OriginId, originId);
            pr.setField(DimId::PointId, pointId);
            ++pointId;

            voxel.initShallow(it.pointRef(), it.data());
            if (so) voxel.clip(*so);
            const Point& point(voxel.point());

            ck.reset();

            if (metadata.boundsConforming.contains(point))
            {
                if (!boundsSubset || boundsSubset->contains(point))
                {
                    key.init(point);
                    cache.insert(voxel, key, ck, clipper);
                    ++counts.inserts;
                }
            }
        }
        counter += counts.inserts;
    });

    json pipeline = info.pipeline.is_null()
        ? json::array({ json::object() })
        : info.pipeline;
    pipeline.at(0)["filename"] = localPath;

    // TODO: Allow this to be disabled via config.
    const bool needsStats = !hasStats(info.schema);
    if (needsStats)
    {
        json& statsFilter = findOrAppendStage(pipeline, "filters.stats");
        if (!statsFilter.count("enumerate"))
        {
            statsFilter.update({ { "enumerate", "Classification" } });
        }
    }

    pdal::PipelineManager pm;
    std::istringstream iss(pipeline.dump());

    std::unique_lock<std::mutex> lock(PdalMutex::get());

    pm.readPipeline(iss);
    pm.validateStageOptions();
    pdal::Stage& last = getStage(pm);
    last.prepare(table);

    lock.unlock();

    last.execute(table);

    // TODO:
    // - update point count information for this file's metadata.
    if (pdal::Stage* stage = findStage(last, "filters.stats"))
    {
        const pdal::StatsFilter& statsFilter(
            dynamic_cast<const pdal::StatsFilter&>(last));

        for (Dimension& d : info.schema)
        {
            const DimId id = layout.findDim(d.name);
            d.stats = DimensionStats(statsFilter.getStats(id));
        }
    }
}

void Builder::save(const unsigned threads)
{
    std::cout << "Saving" << std::endl;
    saveHierarchy(threads);
    saveSources(threads);
    saveMetadata();
}

void Builder::saveHierarchy(const unsigned threads)
{
    const bool monolithic =
        metadata.subset ||
        !std::all_of(manifest.begin(), manifest.end(), isInserted);

    const unsigned step = monolithic ? 0 : hierarchy::determineStep(hierarchy);

    hierarchy::save(
        hierarchy,
        endpoints.hierarchy,
        step,
        threads,
        getPostfix(metadata));
}

void Builder::saveSources(const unsigned threads)
{
    const std::string postfix = getPostfix(metadata);
    const std::string manifestFilename = "manifest" + postfix + ".json";
    const bool pretty = manifest.size() <= 1000;

    if (metadata.subset)
    {
        // If we are a subset, write the whole detailed metadata as one giant
        // blob, since we know we're going to need to wake up the whole thing to
        // do the merge.
        ensurePut(
            endpoints.sources,
            manifestFilename,
            json(manifest).dump(getIndent(pretty)));
    }
    else
    {
        // Save individual per-file metadata.
        manifest = assignMetadataPaths(manifest);
        saveEach(manifest, endpoints.sources, threads, pretty);

        // And in this case, we'll only write an overview for the manifest
        // itself, which excludes things like detailed metadata.
        ensurePut(
            endpoints.sources,
            manifestFilename,
            toOverview(manifest).dump(getIndent(pretty)));
    }
}

void Builder::saveMetadata()
{
    // If we've gained dimension stats during our build, accumulate them and
    // add them to our main metadata.
    const auto pred = [](const BuildItem& b) { return hasStats(b); };
    if (!metadata.subset && std::all_of(manifest.begin(), manifest.end(), pred))
    {
        Schema schema = clearStats(metadata.schema);

        for (const BuildItem& item : manifest)
        {
            auto itemSchema = item.source.info.schema;
            if (auto so = getScaleOffset(metadata.schema))
            {
                itemSchema = setScaleOffset(itemSchema, *so);
            }
            schema = combine(schema, itemSchema, true);
        }

        metadata.schema = schema;
    }

    const std::string postfix = getPostfix(metadata);

    const std::string metaFilename = "ept" + postfix + ".json";
    json metaJson = metadata;
    metaJson["points"] = getInsertedPoints(manifest);
    ensurePut(endpoints.output, metaFilename, metaJson.dump(2));

    const std::string buildFilename = "ept-build" + postfix + ".json";
    ensurePut(endpoints.output, buildFilename, json(metadata.internal).dump(2));
}

namespace builder
{

Builder load(
    const Endpoints endpoints,
    const unsigned threads,
    const unsigned subsetId)
{
    const std::string postfix = subsetId ? "-" + std::to_string(subsetId) : "";
    const json metadataJson = entwine::merge(
        json::parse(endpoints.output.get("ept-build" + postfix + ".json")),
        json::parse(endpoints.output.get("ept" + postfix + ".json")));

    const Metadata metadata = config::getMetadata(metadataJson);

    const Manifest manifest =
        manifest::load(endpoints.sources, threads, postfix);

    const Hierarchy hierarchy =
        hierarchy::load(endpoints.hierarchy, threads, postfix);

    return Builder(endpoints, metadata, manifest, hierarchy);
}

void merge(Builder& dst, const Builder& src, ChunkCache& cache)
{
    // TODO: Should make sure that the src/dst metadata match.  For now we're
    // relying on the user not to have done anything weird.
    const auto& endpoints = dst.endpoints;
    const auto& metadata = dst.metadata;

    Clipper clipper(cache);
    const auto sharedDepth = getSharedDepth(src.metadata);
    for (const auto& node : src.hierarchy.map)
    {
        const Dxyz& key = node.first;
        const uint64_t count = node.second;

        if (key.d >= sharedDepth)
        {
            assert(!hierarchy::get(dst.hierarchy, key));
            hierarchy::set(dst.hierarchy, key, count);
        }
        else
        {
            auto layout = toLayout(metadata.absoluteSchema);
            VectorPointTable table(layout, count);
            table.setProcess([&]()
            {
                Voxel voxel;
                Key pk(metadata.bounds, getStartDepth(metadata));
                ChunkKey ck(metadata.bounds, getStartDepth(metadata));

                for (auto it(table.begin()); it != table.end(); ++it)
                {
                    voxel.initShallow(it.pointRef(), it.data());
                    const Point point(voxel.point());
                    pk.init(point, key.d);
                    ck.init(point, key.d);

                    assert(ck.dxyz() == key);

                    cache.insert(voxel, pk, ck, clipper);
                }
            });

            const auto stem = key.toString() + getPostfix(src.metadata);
            io::read(metadata.dataType, metadata, endpoints, stem, table);
        }
    }
}

} // namespace builder

} // namespace entwine
