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

#include <entwine/types/reprojection.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/types/scale-offset.hpp>
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

json getPipeline(
    const Reprojection* reprojection = nullptr,
    json pipeline = json::array({ json::object() }))
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

std::unique_ptr<ScaleOffset> getScaleOffset(const pdal::Reader& reader)
{
    if (const auto* las = dynamic_cast<const pdal::LasReader*>(&reader))
    {
        const auto& h(las->header());
        return makeUnique<ScaleOffset>(
            Scale(h.scaleX(), h.scaleY(), h.scaleZ()),
            Offset(h.offsetX(), h.offsetY(), h.offsetZ())
        );
    }
    return nullptr;
}

const DimensionDetail* maybeFindDimension(
    const std::vector<DimensionDetail>& dims,
    const std::string& name)
{
    const auto it = std::find_if(
        dims.begin(),
        dims.end(),
        [name](const DimensionDetail& dim) { return dim.name == name; }
    );
    if (it == dims.end()) return nullptr;
    return &*it;
}
DimensionDetail* maybeFindDimension(
    std::vector<DimensionDetail>& dims,
    const std::string& name)
{
    const auto it = std::find_if(
        dims.begin(),
        dims.end(),
        [name](const DimensionDetail& dim) { return dim.name == name; }
    );
    if (it == dims.end()) return nullptr;
    return &*it;
}

DimensionDetail& findDimension(
    std::vector<DimensionDetail>& dims,
    const std::string& name)
{
    DimensionDetail* dim(maybeFindDimension(dims, name));
    if (!dim) throw std::runtime_error("Failed to find dimension: " + name);
    return *dim;
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

PointCloudInfo getInfo(const json pipeline, const bool deep)
{
    PointCloudInfo info;

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
            const DimensionStats stats(statsFilter.getStats(id));
            const DimensionDetail dimension(
                layout.dimName(id),
                layout.dimType(id),
                &stats);
            info.schema.push_back(dimension);
        }

        auto& x = findDimension(info.schema, "X");
        auto& y = findDimension(info.schema, "Y");
        auto& z = findDimension(info.schema, "Z");

        info.metadata = getMetadata(reader);
        if (const auto so = getScaleOffset(reader))
        {
            x.scale = so->scale()[0];
            x.offset = so->offset()[0];
            y.scale = so->scale()[1];
            y.offset = so->offset()[1];
            z.scale = so->scale()[2];
            z.offset = so->offset()[2];
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

    return info;
}

DimensionStats combineStats(DimensionStats agg, const DimensionStats& cur)
{
    agg.minimum = std::min(agg.minimum, cur.minimum);
    agg.maximum = std::max(agg.maximum, cur.maximum);

    // Weighted variance formula from: https://math.stackexchange.com/a/2971563
    double n1 = agg.count;
    double n2 = cur.count;
    double m1 = agg.mean;
    double m2 = cur.mean;
    double v1 = agg.variance;
    double v2 = cur.variance;
    agg.variance =
        (((n1 - 1) * v1) + ((n2 - 1) * v2)) / (n1 + n2 - 1) +
        ((n1 * n2) * (m1 - m2) * (m1 - m2)) / ((n1 + n2) * (n1 + n2 - 1));

    agg.mean = ((agg.mean * agg.count) + (cur.mean * cur.count)) /
        (agg.count + cur.count);
    agg.count += cur.count;
    for (const auto& bucket : cur.values)
    {
        if (!agg.values.count(bucket.first)) agg.values[bucket.first] = 0;
        agg.values[bucket.first] += bucket.second;
    }
    return agg;
}

DimensionDetail combineDimension(DimensionDetail agg, const DimensionDetail& dim)
{
    assert(agg.name == dim.name);
    if (pdal::Dimension::size(dim.type) > pdal::Dimension::size(agg.type))
    {
        agg.type = dim.type;
    }
    agg.scale = std::min(agg.scale, dim.scale);
    // If all offsets are identical we can preserve the offset, otherwise an
    // aggregated offset is meaningless.
    if (agg.offset != dim.offset) agg.offset = 0;

    if (!agg.stats) agg.stats = dim.stats;
    else if (dim.stats)
    {
        agg.stats = std::make_shared<DimensionStats>(
            combineStats(*agg.stats, *dim.stats));
    }
    return agg;
}

DimensionList combineDimensionList(DimensionList agg, const DimensionList& list)
{
    for (const auto& incoming : list)
    {
        auto* current(maybeFindDimension(agg, incoming.name));
        if (!current) agg.push_back(incoming);
        else *current = combineDimension(*current, incoming);
    }
    return agg;
}

PointCloudInfo combineInfo(const PointCloudInfo& agg, const PointCloudInfo& info)
{
    PointCloudInfo output(agg);
    output.errors.insert(
        output.errors.end(),
        info.errors.begin(),
        info.errors.end());
    output.warnings.insert(
        output.warnings.end(),
        info.warnings.begin(),
        info.warnings.end());

    output.metadata = json();
    if (output.srs.empty()) output.srs = info.srs;
    output.bounds.grow(info.bounds);
    output.points += info.points;
    output.schema = combineDimensionList(output.schema, info.schema);

    return output;
}

PointCloudInfo combineSources(const PointCloudInfo& agg, SourceInfo source)
{
    for (auto& warning : source.info.warnings)
    {
        warning = source.path + ": " + warning;
    }
    for (auto& error : source.info.errors)
    {
        error = source.path + ": " + error;
    }
    return combineInfo(agg, source.info);
}

PointCloudInfo reduce(const SourceInfoList& infoList)
{
    return std::accumulate(
        infoList.begin(),
        infoList.end(),
        PointCloudInfo(),
        combineSources);
}

Info::Info(const json& config)
    : m_deep(config.value("deep", false))
    , m_pipeline(
        getPipeline(
            config.count("reprojection")
                ? makeUnique<Reprojection>(config.at("reprojection")).get()
                : nullptr,
            config.value("pipeline", json::array({ json::object() }))
        )
    )
    , m_pool(config.value("threads", 8u))
{
    std::cout << "Info config: " << config.dump(2) << std::endl;
    std::cout << "Pipeline: " << m_pipeline.dump(2) << std::endl;
    for (const auto& path : resolve(config.at("input")))
    {
        m_sources.emplace_back(path);
    }
}

SourceInfoList Info::go()
{
    for (SourceInfo& source : m_sources)
    {
        m_pool.add([this, &source]()
        {
            json pipeline(m_pipeline);
            pipeline.at(0)["filename"] = source.path;
            source.info = getInfo(pipeline, m_deep);
        });
    }

    m_pool.join();

    return m_sources;
}

} // namespace entwine
