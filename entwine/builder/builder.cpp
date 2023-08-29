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
#include <iostream>
#include <limits>

#include <pdal/PipelineManager.hpp>

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/heuristics.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/point-counts.hpp>
#include <entwine/util/config.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/info.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pdal-mutex.hpp>
#include <entwine/util/pipeline.hpp>
#include <entwine/util/time.hpp>

namespace entwine
{
namespace
{

std::string toFullPrecisionString(double d) {
    std::ostringstream os;
    os << 
        std::fixed << 
        std::setprecision(std::numeric_limits<double>::max_digits10) << 
        d;
    return os.str();
}
}

Builder::Builder(
    Endpoints endpoints,
    Metadata metadata,
    Manifest manifest,
    Hierarchy hierarchy,
    bool verbose)
    : endpoints(endpoints)
    , metadata(metadata)
    , manifest(manifest)
    , hierarchy(hierarchy)
    , verbose(verbose)
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
            if (verbose)
            {
                std::cout << "Adding " << origin << " - " << item.source.path <<
                    std::endl;
            }

            pool.add([this, &cache, origin, &counter]()
            {
                tryInsert(cache, origin, counter);
                if (verbose) std::cout << "\tDone " << origin << std::endl;
            });

            ++filesInserted;
        }
    }

    if (verbose) std::cout << "Joining" << std::endl;

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

        const ChunkCache::Info info(ChunkCache::latchInfo());

        if (verbose)
        {
            std::cout << formatTime(tick) << " - " <<
                std::round(progress * 100) << "% - " <<
                commify(inserted) << " - " <<
                commify(pace) << " " <<
                "(" << commify(intervalPace) << ") M/h - " <<
                info.written << "W - " <<
                info.read << "R - " <<
                info.alive << "A" <<
                std::endl;
        }
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

    uint64_t insertedSinceLastSleep(0);
    uint64_t pointId(0);

    // We have our metadata point count - but now we'll count the points that
    // are actually inserted.  If the file's header metadata was inaccurate, or
    // an overabundance of duplicate points causes some to be discarded, then we
    // won't count them.
    info.points = 0;

    auto layout = toLayout(metadata.absoluteSchema);
    VectorPointTable table(layout);
    table.setProcess([&]()
    {
        insertedSinceLastSleep += table.numPoints();
        if (insertedSinceLastSleep > heuristics::sleepCount)
        {
            insertedSinceLastSleep = 0;
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
                    if (cache.insert(voxel, key, ck, clipper)) ++counts.inserts;
                }
            }
        }
        info.points += counts.inserts;
        counter += counts.inserts;
    });

    json pipeline = info.pipeline.is_null()
        ? json::array({ json::object() })
        : info.pipeline;
    pipeline.at(0)["filename"] = localPath;

    if (contains(metadata.schema, "OriginId"))
    {
        pipeline.push_back({
            { "type", "filters.assign" },
            { "value", "OriginId = " + std::to_string(originId) }
        });
    }

    const bool needsStats = !hasStats(info.schema);
    if (needsStats)
    {
        json& statsFilter = findOrAppendStage(pipeline, "filters.stats");
        if (!statsFilter.count("enumerate"))
        {
            statsFilter.update({ { "enumerate", "Classification" } });
        }

        // Only accumulate stats for points that actually get inserted.
        const Bounds b = boundsSubset 
            ? *boundsSubset 
            : metadata.boundsConforming;

        const auto& min = b.min();
        const auto& max = b.max();

        const std::string where = 
            "X >= " + toFullPrecisionString(min.x) + " && " + 
            "X < " + toFullPrecisionString(max.x) + " && " +
            "Y >= " + toFullPrecisionString(min.y) + " && " + 
            "Y < " + toFullPrecisionString(max.y);

        statsFilter.update({ { "where", where } });
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

    if (pdal::Stage* stage = findStage(last, "filters.stats"))
    {
        const pdal::StatsFilter& statsFilter(
            dynamic_cast<const pdal::StatsFilter&>(*stage));

        // Our source file metadata might not have an origin id since we add
        // that dimension.  In that case, add it to the source file's schema so
        // it ends up being included in the stats.
        if (contains(metadata.schema, "OriginId") && 
            !contains(info.schema, "OriginId"))
        {
            info.schema.emplace_back("OriginId", Type::Unsigned32);
        }

        for (Dimension& d : info.schema)
        {
            const DimId id = layout.findDim(d.name);
            d.stats = DimensionStats(statsFilter.getStats(id));
            d.stats->count = info.points;
        }
    }
}

void Builder::save(const unsigned threads)
{
    if (verbose) std::cout << "Saving" << std::endl;
    saveHierarchy(threads);
    saveSources(threads);
    saveMetadata();
}

void Builder::saveHierarchy(const unsigned threads)
{
    // If we are a) saving a subset or b) saving a partial build, then defer
    // choosing a hierarchy step and instead just write one monolothic file.
    const bool isStepped =
        !metadata.subset &&
        std::all_of(manifest.begin(), manifest.end(), isSettled);

    unsigned step = 0;
    if (isStepped)
    {
        if (metadata.internal.hierarchyStep)
        {
            step = metadata.internal.hierarchyStep;
        }
        else step = hierarchy::determineStep(hierarchy);
    }

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
        // do the merge.  In this case, aside from the schema which contains
        // detailed dimension stats for the subset, each corresponding item is 
        // identical per subset: so in that case we will only write the path and
        // schema.
        if (metadata.subset->id != 1)
        {
            json list = json::array();
            for (auto& item : manifest)
            {
                list.push_back({
                    { "path", item.source.path },
                    { "inserted", item.inserted }
                });

                const auto& info = item.source.info;
                auto& j = list.back();

                if (item.inserted)
                {
                    j.update({ { "points", info.points } });

                    if (info.points) j.update({ { "schema", info.schema } });
                }
            }

            ensurePut(
                endpoints.sources,
                manifestFilename,
                list.dump(getIndent(pretty)));
        }
        else
        {
            ensurePut(
                endpoints.sources,
                manifestFilename,
                json(manifest).dump(getIndent(pretty)));
        }
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
    const unsigned subsetId,
    const bool verbose)
{
    const std::string postfix = subsetId ? "-" + std::to_string(subsetId) : "";
    const json metadataJson = entwine::merge(
        json::parse(endpoints.output.get("ept-build" + postfix + ".json")),
        json::parse(endpoints.output.get("ept" + postfix + ".json")));

    const Metadata metadata = config::getMetadata(metadataJson);

    const Manifest manifest =
        manifest::load(endpoints.sources, threads, postfix, verbose);

    const Hierarchy hierarchy =
        hierarchy::load(endpoints.hierarchy, threads, postfix);

    return Builder(endpoints, metadata, manifest, hierarchy);
}

Builder create(json j)
{
    const bool verbose = config::getVerbose(j);
    const Endpoints endpoints = config::getEndpoints(j);
    const unsigned threads = config::getThreads(j);

    Manifest manifest;
    Hierarchy hierarchy;

    // TODO: Handle subset postfixing during existence check - currently
    // continuations of subset builds will not work properly.
    // const optional<Subset> subset = config::getSubset(j);
    if (!config::getForce(j) && endpoints.output.tryGetSize("ept.json"))
    {
        // Merge in our metadata JSON, overriding any config settings.
        const json existingConfig = entwine::merge(
            json::parse(endpoints.output.get("ept-build.json")),
            json::parse(endpoints.output.get("ept.json"))
        );
        j = entwine::merge(j, existingConfig);

        // Awaken our existing manifest and hierarchy.
        manifest = manifest::load(endpoints.sources, threads, "", verbose);
        hierarchy = hierarchy::load(endpoints.hierarchy, threads);
    }

    // Now, analyze the incoming `input` if needed.
    StringList inputs = resolve(config::getInput(j), *endpoints.arbiter);
    const auto exists = [&manifest](std::string path)
    {
        return std::any_of(
            manifest.begin(),
            manifest.end(),
            [path](const BuildItem& b) { return b.source.path == path; });
    };
    // Remove any inputs we already have in our manifest prior to analysis.
    inputs.erase(
        std::remove_if(inputs.begin(), inputs.end(), exists),
        inputs.end());
    const SourceList sources = analyze(
        inputs,
        config::getPipeline(j),
        config::getDeep(j),
        config::getTmp(j),
        *endpoints.arbiter,
        threads,
        verbose);
    for (const auto& source : sources)
    {
        if (source.info.points) manifest.emplace_back(source);
    }

    // It's possible we've just analyzed some files, in which case we have
    // potentially new information like bounds, schema, and SRS.  Prioritize
    // values from the config, which may explicitly override these.
    const SourceInfo analysis = manifest::reduce(sources);
    j = merge(analysis, j);
    const Metadata metadata = config::getMetadata(j);

    return Builder(endpoints, metadata, manifest, hierarchy, verbose);
}

uint64_t run(Builder& builder, const json config)
{
    return builder.run(
        config::getCompoundThreads(config),
        config::getLimit(config),
        config::getProgressInterval(config));
}

void merge(const json config)
{
    merge(
        config::getEndpoints(config),
        config::getThreads(config),
        config::getForce(config),
        config::getVerbose(config));
}

void merge(
    const Endpoints endpoints,
    const unsigned threads,
    const bool force,
    const bool verbose)
{
    if (!force && endpoints.output.tryGetSize("ept.json"))
    {
        throw std::runtime_error(
            "Completed dataset already exists here: "
            "re-run with '--force' to overwrite it");
    }

    if (!endpoints.output.tryGetSize("ept-1.json"))
    {
        throw std::runtime_error("Failed to find first subset");
    }

    if (verbose) std::cout << "Initializing" << std::endl;
    const Builder base = builder::load(endpoints, threads, 1, verbose);

    // Grab the total number of subsets, then clear the subsetting from our
    // metadata aggregator which will represent our merged output.
    Metadata metadata = base.metadata;
    const unsigned of = metadata.subset.value().of;
    metadata.subset = { };

    Manifest manifest = base.manifest;

    Builder builder(endpoints, metadata, manifest, Hierarchy(), verbose);
    ChunkCache cache(endpoints, builder.metadata, builder.hierarchy, threads);

    if (verbose) std::cout << "Merging" << std::endl;

    Pool pool(threads);
    std::mutex mutex;

    for (unsigned id = 1; id <= of; ++id)
    {
        if (verbose) std::cout << "\t" << id << "/" << of << ": ";
        if (endpoints.output.tryGetSize("ept-" + std::to_string(id) + ".json"))
        {
            if (verbose) std::cout << "merging" << std::endl;
            pool.add([
                &endpoints,
                threads,
                verbose,
                id,
                &builder,
                &cache,
                &mutex]()
            {
                Builder current = builder::load(
                    endpoints,
                    threads,
                    id,
                    verbose);
                builder::mergeOne(builder, current, cache);

                // Our base builder contains the manifest of subset 1 so we
                // don't need to merge that one.
                if (id > 1)
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    builder.manifest = manifest::merge(
                        builder.manifest,
                        current.manifest);
                }
            });
        }
        else if (verbose) std::cout << "skipping" << std::endl;
    }

    pool.join();
    cache.join();

    builder.save(threads);
    if (verbose) std::cout << "Done" << std::endl;
}

void mergeOne(Builder& dst, const Builder& src, ChunkCache& cache)
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
        if (!count) continue;

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
