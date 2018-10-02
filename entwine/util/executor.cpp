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
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    class BufferState
    {
    public:
        BufferState(const Bounds& bounds)
            : m_table()
            , m_view(makeUnique<pdal::PointView>(m_table))
            , m_buffer()
        {
            using DimId = pdal::Dimension::Id;

            auto layout(m_table.layout());
            layout->registerDim(DimId::X);
            layout->registerDim(DimId::Y);
            layout->registerDim(DimId::Z);
            layout->finalize();

            std::size_t i(0);

            auto insert([this, &i, &bounds](int x, int y, int z)
            {
                m_view->setField(DimId::X, i, bounds[x ? 0 : 3]);
                m_view->setField(DimId::Y, i, bounds[y ? 1 : 4]);
                m_view->setField(DimId::Z, i, bounds[z ? 2 : 5]);
                ++i;
            });

            insert(0, 0, 0);
            insert(0, 0, 1);
            insert(0, 1, 0);
            insert(0, 1, 1);
            insert(1, 0, 0);
            insert(1, 0, 1);
            insert(1, 1, 0);
            insert(1, 1, 1);

            m_buffer.addView(m_view);
        }

        pdal::PointTable& getTable() { return m_table; }
        pdal::PointView& getView() { return *m_view; }
        pdal::BufferReader& getBuffer() { return m_buffer; }

    private:
        pdal::PointTable m_table;
        pdal::PointViewPtr m_view;
        pdal::BufferReader m_buffer;
    };
}

Executor::Executor()
    : m_stageFactory(makeUnique<pdal::StageFactory>())
{ }

Executor::~Executor()
{ }

bool Executor::good(const std::string path) const
{
    auto ext(arbiter::Arbiter::getExtension(path));
    return !m_stageFactory->inferReaderDriver(path).empty();
}

UniqueStage Executor::createReader(const std::string path) const
{
    UniqueStage result;

    const std::string driver(m_stageFactory->inferReaderDriver(path));
    if (driver.empty()) return result;

    auto lock(getLock());

    if (pdal::Stage* reader = m_stageFactory->createStage(driver))
    {
        pdal::Options options;
        options.add(pdal::Option("filename", path));
        reader->setOptions(options);

        // Unlock before creating the ScopedStage, in case of a throw we can't
        // hold the lock during its destructor.
        lock.unlock();

        result.reset(new ScopedStage(reader, *m_stageFactory, mutex()));
    }

    return result;
}

/*
std::vector<std::string> Executor::dims(const std::string path) const
{
    std::vector<std::string> list;
    UniqueStage scopedReader(createReader(path));
    pdal::Stage* reader(scopedReader->get());
    pdal::PointTable table;
    { auto lock(getLock()); reader->prepare(table); }
    for (const auto& id : table.layout()->dims())
    {
        list.push_back(table.layout()->dimName(id));
    }
    return list;
}
*/

Preview::Preview(pdal::Stage& reader, const pdal::QuickInfo& qi)
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

std::unique_ptr<Preview> Preview::create(pdal::Stage& reader)
{
    const pdal::QuickInfo qi(reader.preview());
    if (qi.valid()) return makeUnique<Preview>(reader, qi);
    return std::unique_ptr<Preview>();
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

std::unique_ptr<Preview> Executor::preview(
        const Json::Value& pipeline,
        const bool trustHeaders) const
{
    Json::Value readerOnlyPipeline;
    readerOnlyPipeline.append(pipeline[0]);
    std::istringstream iss(readerOnlyPipeline.toStyledString());

    auto lock(getLock());
    pdal::PipelineManager pm;
    pm.readPipeline(iss);
    pdal::Stage* reader(pm.getStage());

    std::unique_ptr<Preview> p(Preview::create(*reader));
    lock.unlock();

    if (p)
    {
        // TODO Transform the corners of our QuickInfo's bounds for extrema.
        /*
        if (reprojection)
        {
        }
        */

        if (trustHeaders) return p;
    }
    else
    {
        p = makeUnique<Preview>();
    }

    const Schema xyzSchema({ { DimId::X }, { DimId::Y }, { DimId::Z } });

    // We'll aggregate these from a deep scan of the file instead.
    p->bounds = Bounds::expander();
    p->numPoints = 0;

    VectorPointTable table(xyzSchema);
    table.setProcess([&p, &table]()
    {
        Point point;
        p->numPoints += table.size();

        for (auto it(table.begin()); it != table.end(); ++it)
        {
            auto& pr(it.pointRef());
            point.x = pr.getFieldAs<double>(DimId::X);
            point.y = pr.getFieldAs<double>(DimId::Y);
            point.z = pr.getFieldAs<double>(DimId::Z);
            p->bounds.grow(point);
        }
    });

    if (Executor::get().run(table, pipeline))
    {
        p->dimNames.clear();
        for (const auto& d : xyzSchema.fixedLayout().added())
        {
            p->dimNames.push_back(d);
        }
    }

    return p;
}

std::unique_ptr<Preview> Executor::preview(
        const std::string path,
        const Reprojection* reprojection) const
{
    Json::Value pipeline;

    Json::Value reader;
    reader["filename"] = path;
    pipeline.append(reader);

    if (reprojection)
    {
        Json::Value filter;
        filter["type"] = "filters.reprojection";
        if (!reprojection->in().empty()) filter["in_srs"] = reprojection->in();
        filter["out_srs"] = reprojection->out();
        pipeline.append(filter);
    }

    return preview(pipeline, true);
}

std::string Executor::getSrsString(const std::string input) const
{
    auto lock(getLock());
    return pdal::SpatialReference(input).getWKT();
}

std::unique_lock<std::mutex> Executor::getLock()
{
    return std::unique_lock<std::mutex>(mutex());
}

Reprojection Executor::srsFoundOrDefault(
        const pdal::SpatialReference& found,
        const Reprojection& given)
{
    if (given.hammer() || found.empty()) return given;
    else return Reprojection(found.getWKT(), given.out());
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

