/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "merge.hpp"

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include <entwine/builder/builder.hpp>
#include <entwine/builder/config.hpp>
#include <entwine/types/endpoints.hpp>

namespace entwine
{
namespace app
{

void Merge::addArgs()
{
    m_ap.setUsage("entwine merge <path> (<options>)");

    addOutput("Path containing completed subset builds", true);
    addConfig();
    addTmp();
    addSimpleThreads();
    addArbiter();
    m_ap.add(
            "--force",
            "-f",
            "Force merge overwrite - if a completed EPT dataset exists at this "
            "output location, overwrite it with the result of the merge.",
            [this](json j) { checkEmpty(j); m_json["force"] = true; });
}

void Merge::run()
{
    std::cout << "Merging: " << m_json.dump(2) << std::endl;

    const Endpoints endpoints = config::getEndpoints(m_json);
    const unsigned threads = getTotal(config::getThreads(m_json));
    const bool force = config::getForce(m_json);

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

    std::cout << "Initializing" << std::endl;
    const Builder base = builder::load(endpoints, threads, 1);

    // Grab the total number of subsets, then clear the subsetting from our
    // metadata aggregator which will represent our merged output.
    Metadata metadata = base.metadata;
    const unsigned of = metadata.subset.value().of;
    metadata.subset = { };

    Manifest manifest = base.manifest;

    Builder builder(endpoints, metadata, manifest);
    ChunkCache cache(endpoints, builder.metadata, builder.hierarchy, threads);

    std::cout << "Merging" << std::endl;

    Pool pool(threads);
    std::mutex mutex;

    for (unsigned id = 1; id <= of; ++id)
    {
        std::cout << "\t" << id << "/" << of << ": ";
        if (endpoints.output.tryGetSize("ept-" + std::to_string(id) + ".json"))
        {
            std::cout << "merging" << std::endl;
            pool.add([&endpoints, threads, id, &builder, &cache, &mutex]()
            {
                Builder current = builder::load(endpoints, threads, id);
                builder::merge(builder, current, cache);

                std::lock_guard<std::mutex> lock(mutex);
                builder.manifest = manifest::merge(
                    builder.manifest,
                    current.manifest);
            });
        }
        else std::cout << "skipping" << std::endl;
    }

    pool.join();
    cache.join();

    std::cout << "Saving" << std::endl;
    builder.save(threads);
}

} // namespace app
} // namespace entwine
