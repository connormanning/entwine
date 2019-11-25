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

namespace
{

json objectify(json p)
{
    return json { { "pipeline", p } };
}

json ensureArray(json in)
{
    if (in.is_array() || in.is_null()) return in;
    return json::array({ in });
}

} // unnamed namespace


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
    points = qi.m_pointCount;
    dimNames = qi.m_dimNames;

    if (const auto las = dynamic_cast<pdal::LasReader*>(&reader))
    {
        const auto& h(las->header());
        scale = makeUnique<Scale>(h.scaleX(), h.scaleY(), h.scaleZ());
    }

    metadata = ([&reader]()
    {
        const auto s(pdal::Utils::toJSON(reader.getMetadata()));
        try { return json::parse(s); }
        catch (...) { return json(); }
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
        const json rawPipeline,
        const bool shallow) const
{
    const json pipeline = ensureArray(rawPipeline);

    if (!shallow) return deepScan(pipeline);

    std::unique_ptr<ScanInfo> result;
    json readerJson(pipeline.at(0));

    auto lock(getLock());

    pdal::SpatialReference activeSrs;
    {
        // First get the active SRS from a fully-specified reader - it may be
        // overridden or defaulted here.  We'll need this SRS result to
        // reproject our extents later.
        pdal::PipelineManager pm;
        std::istringstream readStream(objectify(ensureArray(readerJson)).dump());
        pm.readPipeline(readStream);
        pdal::Stage* reader(pm.getStage());

        pdal::FixedPointTable table(0);
        reader->prepare(table);
        activeSrs = reader->getSpatialReference();
    }

    {
        // Now remove the SRS overrides so we store the true SRS of this file
        // in our metadata.  We still want any other options though - they may
        // be necessary for a proper preview - for example CSV or GDAL column
        // mappings.
        if (readerJson.is_object())
        {
            readerJson.erase("override_srs");
            readerJson.erase("default_srs");
            readerJson.erase("spatialreference");
        }

        pdal::PipelineManager pm;
        std::istringstream readStream(objectify(ensureArray(readerJson)).dump());
        pm.readPipeline(readStream);
        pdal::Stage* reader(pm.getStage());

        result = ScanInfo::create(*reader);
    }

    lock.unlock();

    if (!result)
    {
        static bool logged(false);
        if (!logged)
        {
            logged = true;
            std::cout << "Shallow preview failed - falling back to full read" <<
                std::endl;
        }
        return deepScan(pipeline, true);
    }

    const json filters(pipeline.begin() + 1, pipeline.end());
    if (filters.empty()) return result;

    lock.lock();

    // We've gotten our initial ScanInfo - but our bounds might not be accurate
    // to the output.  For example, a reprojection filter will mean our bounds
    // are in the wrong SRS.  We'll run the 8 corners of our extents through
    // the entire pipeline and take the resulting extents.  For user-supplied
    // pipelines where this assumption does not hold, the onus is on the user
    // to specify a deep scan which will pipeline every point.

    pdal::PipelineManager pm;
    std::istringstream filterStream(objectify(filters).dump());
    pm.readPipeline(filterStream);
    pdal::Stage* last(pm.getStage());
    pdal::Stage* first(last);
    while (first->getInputs().size())
    {
        if (first->getInputs().size() > 1)
        {
            throw std::runtime_error("Invalid pipeline - must be linear");
        }

        first = first->getInputs().at(0);
    }

    DimList dims;
    for (const std::string name : result->dimNames) dims.emplace_back(name);
    const Schema schema(dims);
    VectorPointTable table(schema, 8);
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

std::unique_ptr<ScanInfo> Executor::deepScan(
    const json pipeline,
    const bool fallback) const
{
    // Start with a shallow scan to get SRS, scale, and metadata.
    std::unique_ptr<ScanInfo> result;
    if (!fallback) result = preview(pipeline, true);
    if (!result) result = makeUnique<ScanInfo>();

    DimList dims { { DimId::X }, { DimId::Y }, { DimId::Z } };
    for (const std::string name : result->dimNames)
    {
        if (name != "X" && name != "Y" && name != "Z") dims.emplace_back(name);
    }
    const Schema schema(dims);

    // Reset the values we're going to aggregate from the deep scan.
    result->bounds = Bounds::expander();
    result->points = 0;
    result->dimNames.clear();

    VectorPointTable table(schema);
    table.setProcess([&result, &table]()
    {
        Point point;

        for (auto it(table.begin()); it != table.end(); ++it)
        {
            // Don't use table.numPoints since that includes skipped points.
            ++result->points;

            auto& pr(it.pointRef());
            point.x = pr.getFieldAs<double>(DimId::X);
            point.y = pr.getFieldAs<double>(DimId::Y);
            point.z = pr.getFieldAs<double>(DimId::Z);
            result->bounds.grow(point);
        }
    });

    if (Executor::get().run(table, pipeline))
    {
        for (const auto& d : schema.fixedLayout().added())
        {
            result->dimNames.push_back(d);
        }
        return result;
    }
    else return std::unique_ptr<ScanInfo>();
}

bool Executor::run(pdal::StreamPointTable& table, const json pipeline)
{
    std::istringstream iss(objectify(pipeline).dump());

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
        static bool logged(false);
        if (!logged)
        {
            logged = true;
            std::cout << "Using non-streaming mode" << std::endl;
        }
        pm.prepare();
        lock.unlock();

        pm.execute();

        pdal::PointRef pr(table, 0);

        uint64_t current(0);
        for (auto& view : pm.views())
        {
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

