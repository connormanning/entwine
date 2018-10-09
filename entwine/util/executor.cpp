/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/executor.hpp>

#include <sstream>

#include <pdal/Dimension.hpp>
#include <pdal/QuickInfo.hpp>
#include <pdal/SpatialReference.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/io/BufferReader.hpp>
#include <pdal/io/LasReader.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Executor::Executor()
    : m_stageFactory(makeUnique<pdal::StageFactory>())
{ }

Executor::~Executor()
{ }

bool Executor::good(const std::string path) const
{
    auto ext(arbiter::Arbiter::getExtension(path));
    return ext != "txt" && !m_stageFactory->inferReaderDriver(path).empty();
}

ScanInfo::ScanInfo(pdal::Stage& reader, const pdal::QuickInfo& qi)
{
    if (!qi.m_bounds.empty())
    {
        bounds = Bounds(
                qi.m_bounds.minx, qi.m_bounds.miny, qi.m_bounds.minz,
                qi.m_bounds.maxx, qi.m_bounds.maxy, qi.m_bounds.maxz);
    }

    srs = qi.m_srs.getWKT();
    numPoints = qi.m_pointCount;
    dimNames = qi.m_dimNames;

    if (const auto las = dynamic_cast<pdal::LasReader*>(&reader))
    {
        const auto& h(las->header());
        scale = makeUnique<Scale>(h.scaleX(), h.scaleY(), h.scaleZ());
    }

    metadata = ([this, &reader]()
    {
        const auto s(pdal::Utils::toJSON(reader.getMetadata()));
        try { return parse(s); }
        catch (...) { return Json::Value(s); }
    })();
}

std::unique_ptr<ScanInfo> ScanInfo::create(pdal::Stage& reader)
{
    const pdal::QuickInfo qi(reader.preview());
    if (qi.valid()) return makeUnique<ScanInfo>(reader, qi);
    return std::unique_ptr<ScanInfo>();
}

class StreamReader : public pdal::Reader, public pdal::Streamable
{
public:
    StreamReader(pdal::StreamPointTable& table) : m_table(table) { }

    std::string getName() const { return "readers.stream"; }

private:
    virtual bool processOne(pdal::PointRef&)
    {
        return ++m_index <= m_table.capacity();
    }

    pdal::point_count_t m_index = 0;
    pdal::StreamPointTable& m_table;
};

std::unique_ptr<ScanInfo> Executor::preview(
        Json::Value pipeline,
        const bool trustHeaders) const
{
    if (!trustHeaders) return deepScan(pipeline);

    pipeline = ensureArray(pipeline);

    std::unique_ptr<ScanInfo> result;

    auto lock(getLock());
    auto pm = makeUnique<pdal::PipelineManager>();

    Json::Value readerJson(ensureArray(pipeline[0]));

    pdal::SpatialReference activeSrs;
    {
        pdal::FixedPointTable table(0);
        std::istringstream readStream(readerJson.toStyledString());
        pm->readPipeline(readStream);
        pdal::Stage* reader(pm->getStage());
        // reader->prepare(table);
        result = ScanInfo::create(*reader);
        if (result) activeSrs = result->srs;
    }

    {
        pm = makeUnique<pdal::PipelineManager>();
        if (readerJson[0].isObject()) readerJson[0] = readerJson[0]["filename"];
        std::istringstream readStream(readerJson.toStyledString());
        pm->readPipeline(readStream);
        pdal::Stage* reader(pm->getStage());
        if (auto s = ScanInfo::create(*reader)) result->srs = s->srs;
    }

    lock.unlock();

    if (!result) return result;

    const Json::Value filters(slice(pipeline, 1));
    if (filters.isNull()) return result;

    lock.lock();

    // We've gotten our initial ScanInfo - but our bounds might not be accurate
    // to the output.  For example, a reprojection filter will mean our bounds
    // are in the wrong SRS.  We'll run the 8 corners of our extents through
    // the entire pipeline and take the resulting extents.  For user-supplied
    // pipelines where this assumption does not hold, the onus is on the user
    // to specify a deep scan which will pipeline every point.

    pm = makeUnique<pdal::PipelineManager>();
    std::istringstream filterStream(filters.toStyledString());
    pm->readPipeline(filterStream);
    pdal::Stage* last(pm->getStage());
    pdal::Stage* first(last);
    while (first->getInputs().size())
    {
        if (first->getInputs().size() > 1)
        {
            throw std::runtime_error("Invalid pipeline - must be linear");
        }

        first = first->getInputs().at(0);
    }

    const Schema xyzSchema({ { DimId::X }, { DimId::Y }, { DimId::Z } });
    VectorPointTable table(xyzSchema, 8);
    table.setProcess([&table, &result]()
    {
        Point point;
        for (auto it(table.begin()); it != table.end(); ++it)
        {
            auto& pr(it.pointRef());
            point.x = pr.getFieldAs<double>(DimId::X);
            point.y = pr.getFieldAs<double>(DimId::Y);
            point.z = pr.getFieldAs<double>(DimId::Z);
            result->bounds.grow(point);
        }
    });

    for (int i(0); i < 8; ++i)
    {
        auto pr(table.at(i));
        pr.setField(DimId::X, result->bounds[0 + (i & 1 ? 0 : 3)]);
        pr.setField(DimId::Y, result->bounds[1 + (i & 2 ? 0 : 3)]);
        pr.setField(DimId::Z, result->bounds[2 + (i & 4 ? 0 : 3)]);
    }
    result->bounds = Bounds::expander();

    StreamReader streamReader(table);
    streamReader.setSpatialReference(activeSrs);
    first->setInput(streamReader);
    last->prepare(table);
    last->execute(table);

    return result;
}

std::unique_ptr<ScanInfo> Executor::deepScan(Json::Value pipeline) const
{
    pipeline = ensureArray(pipeline);

    // Start with a shallow scan to get SRS, scale, and metadata.
    std::unique_ptr<ScanInfo> result(preview(pipeline, true));
    if (!result) result = makeUnique<ScanInfo>();

    const Schema xyzSchema({ { DimId::X }, { DimId::Y }, { DimId::Z } });

    // Reset the values we're going to aggregate from the deep scan.
    result->bounds = Bounds::expander();
    result->numPoints = 0;
    result->dimNames.clear();

    VectorPointTable table(xyzSchema);
    table.setProcess([&result, &table]()
    {
        Point point;
        result->numPoints += table.size();

        for (auto it(table.begin()); it != table.end(); ++it)
        {
            auto& pr(it.pointRef());
            point.x = pr.getFieldAs<double>(DimId::X);
            point.y = pr.getFieldAs<double>(DimId::Y);
            point.z = pr.getFieldAs<double>(DimId::Z);
            result->bounds.grow(point);
        }
    });

    if (Executor::get().run(table, pipeline))
    {
        for (const auto& d : xyzSchema.fixedLayout().added())
        {
            result->dimNames.push_back(d);
        }
        return result;
    }
    else return std::unique_ptr<ScanInfo>();
}

bool Executor::run(pdal::StreamPointTable& table, const Json::Value& pipeline)
{
    std::istringstream iss(pipeline.toStyledString());

    auto lock(getLock());
    pdal::PipelineManager pm;
    pm.readPipeline(iss);

    if (pm.pipelineStreamable())
    {
        pm.validateStageOptions();
        pdal::Stage *s = pm.getStage();
        if (!s) return false;
        s->prepare(table);

        lock.unlock();
        s->execute(table);
    }
    else
    {
        // TODO.
        throw std::runtime_error("Only streaming for now...");
    }

    return true;
}

std::unique_lock<std::mutex> Executor::getLock()
{
    return std::unique_lock<std::mutex>(mutex());
}

ScopedStage::ScopedStage(
        pdal::Stage* stage,
        pdal::StageFactory& stageFactory,
        std::mutex& factoryMutex)
    : m_stage(stage)
    , m_stageFactory(stageFactory)
    , m_factoryMutex(factoryMutex)
{ }

ScopedStage::~ScopedStage()
{
    std::lock_guard<std::mutex> lock(m_factoryMutex);
    m_stageFactory.destroyStage(m_stage);
}

} // namespace entwine

