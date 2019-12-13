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

#include <pdal/PointTable.hpp>
#include <pdal/io/LasReader.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

json& findOrAppendStage(json& pipeline, std::string type)
{
    auto it = std::find_if(
        pipeline.begin(),
        pipeline.end(),
        [type](const json& stage) { return stage.value("type", "") == type; });

    if (it != pipeline.end()) return *it;

    pipeline.push_back({ { "type", type } });
    return pipeline.back();
}

bool isDirectory(std::string path)
{
    if (path.empty()) throw std::runtime_error("Cannot specify empty path");
    const char c = path.back();
    return c == '/' || c == '\\' || c == '*' ||
        arbiter::getExtension(path).empty();
}

// Accepts an array of inputs which are some combination of file/directory
// paths.  Input paths which are directories are globbed into their constituent
// files.
StringList resolve(
    const StringList& input,
    const arbiter::Arbiter& a = arbiter::Arbiter())
{
    StringList output;
    for (std::string item : input)
    {
        if (isDirectory(item))
        {
            const char last = item.back();
            if (last != '*')
            {
                if (last != '/') item.push_back('/');
                item.push_back('*');
            }

            const StringList directory(a.resolve(item));
            for (const auto& item : directory)
            {
                if (!isDirectory(item)) output.push_back(item);
            }
        }
        else output.push_back(item);
    }
    return output;
}

void runNonStreaming(pdal::PipelineManager& pm, pdal::StreamPointTable& table)
{
    auto lock(Executor::getLock());
    pm.prepare();
    lock.unlock();

    pm.execute();

    pdal::PointRef pr(table, 0);

    uint64_t current(0);
    for (auto& view : pm.views())
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

void runStreaming(pdal::PipelineManager& pm, pdal::StreamPointTable& table)
{
    auto lock(Executor::getLock());
    pdal::Stage *s = pm.getStage();
    s->prepare(table);
    lock.unlock();

    s->execute(table);
}

void run(pdal::PipelineManager& pm, pdal::StreamPointTable& table)
{
    if (pm.pipelineStreamable()) runStreaming(pm, table);
    else runNonStreaming(pm, table);
}

const pdal::Reader& getReader(pdal::Stage& last)
{
    pdal::Stage* first(&last);
    while (first->getInputs().size())
    {
        if (first->getInputs().size() > 1)
        {
            throw std::runtime_error("Invalid pipeline - must be linear");
        }

        first = first->getInputs().at(0);
    }

    const pdal::Reader* reader(dynamic_cast<const pdal::Reader*>(first));
    if (!reader)
    {
        throw std::runtime_error("Invalid pipeline - must start with reader");
    }

    return *reader;
}

json getMetadata(const pdal::Reader& reader)
{
    return json::parse(pdal::Utils::toJSON(reader.getMetadata()));
}

optional<ScaleOffset> getScaleOffset(const pdal::Reader& reader)
{
    if (const auto* las = dynamic_cast<const pdal::LasReader*>(&reader))
    {
        const auto& h(las->header());
        return ScaleOffset(
            Scale(h.scaleX(), h.scaleY(), h.scaleZ()),
            Offset(h.offsetX(), h.offsetY(), h.offsetZ())
        );
    }
    return { };
}

class CountingPointTable : public pdal::FixedPointTable
{
public:
    CountingPointTable() : pdal::FixedPointTable(4096) { }

    uint64_t total() const { return m_total; }

protected:
    void reset() override
    {
        for (uint64_t i(0); i < numPoints(); ++i)
        {
            if (!skip(i)) ++m_total;
        }
        pdal::FixedPointTable::reset();
    }

    uint64_t m_total = 0;
};

source::Info getInfo(const json pipeline, const bool deep)
{
    source::Info info;

    try
    {
        CountingPointTable table;
        std::istringstream iss(pipeline.dump());

        auto lock(Executor::getLock());
        pdal::PipelineManager pm;
        pm.readPipeline(iss);
        pm.validateStageOptions();
        lock.unlock();

        // Extract stats filter from the pipeline.
        const pdal::Stage* last(pm.getStage());
        if (!last) throw std::runtime_error("Invalid pipeline - no stages");
        if (last->getName() != "filters.stats")
        {
            throw std::runtime_error(
                "Invalid pipeline - must end with filters.stats");
        }
        const pdal::StatsFilter& statsFilter(
            dynamic_cast<const pdal::StatsFilter&>(*last));

        const pdal::Reader& reader(getReader(*pm.getStage()));

        run(pm, table);

        const auto& layout(*table.layout());
        for (const DimId id : layout.dims())
        {
            const dimension::Stats stats(statsFilter.getStats(id));
            const dimension::Dimension dimension(
                layout.dimName(id),
                layout.dimType(id),
                stats);
            info.dimensions.push_back(dimension);
        }

        auto& x = dimension::find(info.dimensions, "X");
        auto& y = dimension::find(info.dimensions, "Y");
        auto& z = dimension::find(info.dimensions, "Z");

        info.metadata = getMetadata(reader);
        if (const auto so = getScaleOffset(reader))
        {
            x.scale = so->scale()[0];
            x.offset = so->offset()[0];

            y.scale = so->scale()[1];
            y.offset = so->offset()[1];

            z.scale = so->scale()[2];
            z.offset = so->offset()[2];

            x.type = DimType::Signed32;
            y.type = DimType::Signed32;
            z.type = DimType::Signed32;
        }
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
    catch (std::exception& e)
    {
        info.errors.push_back(e.what());
    }
    catch (...)
    {
        info.errors.push_back("Unknown error");
    }

    return info;
}

std::string getStem(std::string path)
{
    return arbiter::stripExtension(arbiter::getBasename(path));
}

bool areBasenamesUnique(const source::List& sources)
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

json createInfoPipeline(json pipeline, optional<Reprojection> reprojection)
{
    if (pipeline.is_object()) pipeline = pipeline.at("pipeline");

    if (!pipeline.is_array() || pipeline.empty())
    {
        throw std::runtime_error("Invalid pipeline: " + pipeline.dump(2));
    }

    json& reader(pipeline.at(0));

    // Configure the reprojection stage, if applicable.
    if (reprojection)
    {
        // First set the input SRS on the reader if necessary.
        const std::string in(reprojection->in());
        if (in.size())
        {
            if (reprojection->hammer()) reader["override_srs"] = in;
            else reader["default_srs"] = in;
        }

        // Now set up the output.  If there's already a filters.reprojection in
        // the pipeline, we'll fill it in.  Otherwise, we'll append one.
        json& filter(findOrAppendStage(pipeline, "filters.reprojection"));
        filter.update({ {"out_srs", reprojection->out() } });
    }

    // Finally, append a stats filter to the end of the pipeline.
    {
        json& filter(findOrAppendStage(pipeline, "filters.stats"));
        if (!filter.count("enumerate"))
        {
            filter.update({ { "enumerate", "Classification" } });
        }
    }

    return pipeline;
}

json extractInfoPipelineFromConfig(json config)
{
    const auto pipeline = config.value(
        "pipeline",
        json::array({ json::object() }));

    optional<Reprojection> reprojection(config.value("reprojection", json()));

    return createInfoPipeline(pipeline, reprojection);
}

source::List analyze(
    const json& pipelineTemplate,
    const StringList& inputs,
    const unsigned int threads)
{
    const StringList flattened(resolve(inputs));
    source::List sources(flattened.begin(), flattened.end());
    Pool pool(threads);
    for (source::Source& source : sources)
    {
        auto pipeline = pipelineTemplate;
        pipeline.at(0)["filename"] = source.path;

        pool.add([&source, pipeline]()
        {
            source.info = getInfo(pipeline, true);
        });
    }

    pool.join();
    return sources;
}

source::List analyze(const json& config)
{
    const json pipeline = extractInfoPipelineFromConfig(config);
    const StringList inputs = config.at("input");
    const unsigned int threads = config.value("threads", 8u);
    return analyze(pipeline, inputs, threads);
}

void serialize(
    const source::List& sources,
    const arbiter::Endpoint& ep,
    const unsigned int threads)
{
    const bool basenamesUnique = areBasenamesUnique(sources);

    uint64_t i(0);
    Pool pool(threads);
    for (const source::Source& source : sources)
    {
        const std::string stem = basenamesUnique
            ? getStem(source.path)
            : std::to_string(i);

        pool.add([&ep, &source, stem]()
        {
            ep.put(stem + ".json", json(source).dump(2));
        });

        ++i;
    }
    pool.join();
}

} // namespace entwine
