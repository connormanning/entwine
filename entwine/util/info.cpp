/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "info.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <numeric>

#include <pdal/io/BufferReader.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/PointTable.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pdal-mutex.hpp>
#include <entwine/util/pipeline.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

void executeStandard(pdal::Stage& s, pdal::StreamPointTable& table)
{
    pdal::PointTable standardTable;
    s.prepare(standardTable);

    pdal::PointRef pr(table);
    uint64_t current(0);
    for (auto& view : s.execute(standardTable))
    {
        table.setSpatialReference(view->spatialReference());
        pr.setPointId(current);
        for (uint64_t i(0); i < view->size(); ++i)
        {
            pr.setPackedData(view->dimTypes(), view->getPoint(i));

            if (++current == table.capacity())
            {
                table.clear(table.capacity());
                current = 0;
            }
        }
    }
    if (current) table.clear(current);
}

void executeStreaming(pdal::Stage& s, pdal::StreamPointTable& table)
{
    {
        std::lock_guard<std::mutex> lock(PdalMutex::get());
        s.prepare(table);
    }
    s.execute(table);
}

void execute(pdal::Stage& s, pdal::StreamPointTable& table)
{
    if (s.pipelineStreamable()) executeStreaming(s, table);
    else executeStandard(s, table);
}

SourceInfo getShallowInfo(const json pipeline)
{
    SourceInfo info;
    info.pipeline = pipeline;

    const json filterJson = slice(pipeline, 1);

    std::unique_lock<std::mutex> lock(PdalMutex::get());

    pdal::PipelineManager pm;
    std::istringstream iss(pipeline.dump());
    pm.readPipeline(iss);
    pm.validateStageOptions();

    pdal::Stage& stage = getStage(pm);
    pdal::Reader& reader(getReader(stage));
    const bool streamable = stage.pipelineStreamable();
    if (!streamable) info.warnings.push_back("Pipeline is not streamable");

    const pdal::QuickInfo qi(reader.preview());
    if (!qi.valid()) throw ShallowInfoError("Failed to extract info");
    if (qi.m_bounds.empty()) throw ShallowInfoError("Failed to extract bounds");

    pdal::PointTable table;
    stage.prepare(table);

    lock.unlock();

    info.schema = fromLayout(*table.layout());
    if (const auto so = getScaleOffset(reader))
    {
        info.schema = setScaleOffset(info.schema, *so);
    }

    info.points = qi.m_pointCount;
    info.metadata = getMetadata(reader);

    const auto& qb(qi.m_bounds);
    const Bounds native(qb.minx, qb.miny, qb.minz, qb.maxx, qb.maxy, qb.maxz);
    const Srs readerSrs(qi.m_srs.getWKT());

    // If we have filters in our pipeline, these won't necessarily be correct -
    // we'll handle that shortly.
    info.srs = readerSrs;
    info.bounds = native;

    if (filterJson.empty()) return info;

    // We've got most of what we need, except that our bounds and SRS may be
    // altered by a reprojection or other transformation.  So we'll flow the
    // extents of our reader's native bounds through the rest of the pipeline
    // and see what comes out.
    auto view = std::make_shared<pdal::PointView>(table); // , qi.m_srs);
    for (int i(0); i < 8; ++i)
    {
        view->setField(DimId::X, i, native[0 + (i & 1 ? 0 : 3)]);
        view->setField(DimId::Y, i, native[1 + (i & 2 ? 0 : 3)]);
        view->setField(DimId::Z, i, native[2 + (i & 4 ? 0 : 3)]);
    }
    pdal::BufferReader bufferReader;
    bufferReader.setSpatialReference(qi.m_srs);
    bufferReader.addView(view);

    {
        lock.lock();

        pdal::PipelineManager pm;
        std::istringstream iss(filterJson.dump());
        pm.readPipeline(iss);
        pm.validateStageOptions();

        pdal::Stage& last = getStage(pm);
        getFirst(last).setInput(bufferReader);
        last.prepare(table);

        lock.unlock();

        auto result = *last.execute(table).begin();

        info.bounds = Bounds::expander();
        for (uint64_t i(0); i < result->size(); ++i)
        {
            info.bounds.grow(
                Point(
                    result->getFieldAs<double>(DimId::X, i),
                    result->getFieldAs<double>(DimId::Y, i),
                    result->getFieldAs<double>(DimId::Z, i)));
        }

        info.srs = Srs(result->spatialReference().getWKT());
    }

    return info;
}

SourceInfo getDeepInfo(json pipeline)
{
    SourceInfo info;
    info.pipeline = pipeline;

    {
        json& filter(findOrAppendStage(pipeline, "filters.stats"));
        if (!filter.count("enumerate"))
        {
            filter.update({ { "enumerate", "Classification" } });
        }
    }

    try
    {
        std::unique_lock<std::mutex> lock(PdalMutex::get());
        pdal::PipelineManager pm;
        std::istringstream iss(pipeline.dump());
        pm.readPipeline(iss);
        pm.validateStageOptions();
        if (!pm.pipelineStreamable())
        {
            info.warnings.push_back("Pipeline is not streamable");
        }
        lock.unlock();

        // Extract stats filter from the pipeline.
        pdal::Stage& last(getStage(pm));
        if (last.getName() != "filters.stats")
        {
            throw std::runtime_error(
                "Invalid pipeline - must end with filters.stats");
        }
        const pdal::StatsFilter& statsFilter(
            dynamic_cast<const pdal::StatsFilter&>(last));

        pdal::Reader& reader(getReader(getStage(pm)));

        pdal::FixedPointTable table(4096);
        execute(last, table);

        const auto& layout(*table.layout());
        for (const DimId id : layout.dims())
        {
            const DimensionStats stats(statsFilter.getStats(id));
            const Dimension dimension(
                layout.dimName(id),
                layout.dimType(id),
                stats);
            info.schema.push_back(dimension);
        }

        info.metadata = getMetadata(reader);
        if (const auto so = getScaleOffset(reader))
        {
            info.schema = setScaleOffset(info.schema, *so);
        }

        auto& x = find(info.schema, "X");
        auto& y = find(info.schema, "Y");
        auto& z = find(info.schema, "Z");
        info.bounds = Bounds(
            x.stats->minimum,
            y.stats->minimum,
            z.stats->minimum,
            x.stats->maximum,
            y.stats->maximum,
            z.stats->maximum);
        info.points = x.stats->count;
        info.srs = Srs(table.anySpatialReference().getWKT());
    }
    catch (const std::exception& e)
    {
        info.errors.push_back(e.what());
    }
    catch (...)
    {
        info.errors.push_back("Unknown error");
    }

    return info;
}

bool areStemsUnique(const SourceList& sources)
{
    std::set<std::string> set;
    for (const auto& source : sources)
    {
        const std::string stem = getStem(source.path);
        if (set.count(stem)) return false;
        set.insert(stem);
    }
    return true;
}

SourceInfo analyzeOne(const std::string path, const bool deep, json pipeline)
{
    try
    {
        pipeline.at(0)["filename"] = path;
        return deep ? getDeepInfo(pipeline) : getShallowInfo(pipeline);
    }
    catch (const std::exception& e)
    {
        SourceInfo info;
        info.errors.push_back(std::string("Failed to analyze: ") + e.what());
        return info;
    }
    catch (...)
    {
        SourceInfo info;
        info.errors.push_back("Failed to analyze");
        return info;
    }
}

Source parseOne(const std::string path, const arbiter::Arbiter& a)
{
    Source source(path);
    try
    {
        const json j(json::parse(a.get(path)));

        // Note that we're overwriting our JSON filename here with
        // the path to the actual point cloud.
        source.path = j.at("path").get<std::string>();
        source.info = j.get<SourceInfo>();
    }
    catch (const std::exception& e)
    {
        source.info.errors.push_back(
            std::string("Failed to fetch info: ") + e.what());
    }
    catch (...)
    {
        source.info.errors.push_back("Failed to fetch info");
    }

    return source;
}

std::string toLower(std::string s)
{
    std::transform(
        s.begin(),
        s.end(),
        s.begin(),
        [](unsigned char c) { return std::tolower(c); });
    return s;
}

arbiter::LocalHandle localize(
    const std::string path,
    const bool deep,
    const std::string tmp,
    const arbiter::Arbiter& a)
{
    const std::string extension = toLower(arbiter::getExtension(path));
    const bool isLas = extension == "las" || extension == "laz";
    if (deep || a.isLocal(path) || !isLas) return a.getLocalHandle(path, tmp);
    return getPointlessLasFile(path, tmp, a);
}

SourceList analyze(
    const StringList& inputs,
    const json& pipelineTemplate,
    const bool deep,
    const std::string tmp,
    const arbiter::Arbiter& a,
    const unsigned int threads,
    const bool verbose)
{
    const StringList filenames = resolve(inputs);
    SourceList sources(filenames.begin(), filenames.end());

    uint64_t i(0);

    Pool pool(threads);
    for (Source& source : sources)
    {
        if (verbose)
        {
            std::cout << ++i << "/" << sources.size() << ": " << source.path <<
                std::endl;
        }

        if (arbiter::getExtension(source.path) == "json")
        {
            pool.add([&source, &a]()
            {
                source = parseOne(source.path, a);
            });
        }
        else
        {
            pool.add([&]()
            {
                const auto handle(localize(source.path, deep, tmp, a));
                source.info = analyzeOne(
                    handle.localPath(),
                    deep,
                    pipelineTemplate);
            });
        }
    }
    pool.join();

    return sources;
}

} // namespace entwine
