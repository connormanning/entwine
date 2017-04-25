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

#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/schema.hpp>
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

            m_view->setField(DimId::X, i, bounds.min().x);
            m_view->setField(DimId::Y, i, bounds.min().y);
            m_view->setField(DimId::Z, i, bounds.min().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.min().x);
            m_view->setField(DimId::Y, i, bounds.min().y);
            m_view->setField(DimId::Z, i, bounds.max().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.max().x);
            m_view->setField(DimId::Y, i, bounds.max().y);
            m_view->setField(DimId::Z, i, bounds.min().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.max().x);
            m_view->setField(DimId::Y, i, bounds.max().y);
            m_view->setField(DimId::Z, i, bounds.max().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.min().x);
            m_view->setField(DimId::Y, i, bounds.max().y);
            m_view->setField(DimId::Z, i, bounds.min().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.min().x);
            m_view->setField(DimId::Y, i, bounds.max().y);
            m_view->setField(DimId::Z, i, bounds.max().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.max().x);
            m_view->setField(DimId::Y, i, bounds.min().y);
            m_view->setField(DimId::Z, i, bounds.min().z);
            ++i;

            m_view->setField(DimId::X, i, bounds.max().x);
            m_view->setField(DimId::Y, i, bounds.min().y);
            m_view->setField(DimId::Z, i, bounds.max().z);
            ++i;

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

std::vector<std::string> Executor::dims(const std::string path) const
{
    std::vector<std::string> list;
    UniqueStage scopedReader(createReader(path));
    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    pdal::PointTable table;
    { auto lock(getLock()); reader->prepare(table); }
    for (const auto& id : table.layout()->dims())
    {
        list.push_back(table.layout()->dimName(id));
    }
    return list;
}

std::unique_ptr<Preview> Executor::preview(
        const std::string path,
        const Reprojection* reprojection)
{
    std::unique_ptr<Preview> result;

    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return result;

    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    const pdal::QuickInfo qi(([this, reader]()
    {
        auto lock(getLock());
        pdal::QuickInfo q = reader->preview();
        return q;
    })());

    const Json::Value metadata(([this, reader]()
    {
        auto lock(getLock());
        const auto s(pdal::Utils::toJSON(reader->getMetadata()));
        try { return parse(s); }
        catch (...) { return Json::Value(s); }
    })());

    if (!qi.valid() || qi.m_bounds.empty()) return result;

    std::unique_ptr<Scale> scale;
    Bounds bounds(
            Point(qi.m_bounds.minx, qi.m_bounds.miny, qi.m_bounds.minz),
            Point(qi.m_bounds.maxx, qi.m_bounds.maxy, qi.m_bounds.maxz));

    if (const auto lasReader = dynamic_cast<pdal::LasReader*>(reader))
    {
        const auto& header(lasReader->header());
        scale = makeUnique<Scale>(
                header.scaleX(),
                header.scaleY(),
                header.scaleZ());
    }

    std::string srs;

    if (!reprojection)
    {
        srs = qi.m_srs.getWKT();
    }
    else
    {
        using DimId = pdal::Dimension::Id;

        BufferState bufferState(bounds);

        const auto selectedSrs(
                srsFoundOrDefault(qi.m_srs, *reprojection));

        UniqueStage scopedFilter(createReprojectionFilter(selectedSrs));
        if (!scopedFilter) return result;

        pdal::Filter& filter(*scopedFilter->getAs<pdal::Filter*>());

        filter.setInput(bufferState.getBuffer());
        { auto lock(getLock()); filter.prepare(bufferState.getTable()); }
        filter.execute(bufferState.getTable());

        Bounds b(Bounds::expander());

        for (std::size_t i(0); i < bufferState.getView().size(); ++i)
        {
            const Point p(
                    bufferState.getView().getFieldAs<double>(DimId::X, i),
                    bufferState.getView().getFieldAs<double>(DimId::Y, i),
                    bufferState.getView().getFieldAs<double>(DimId::Z, i));

            b.grow(p);
        }

        bounds = b;

        auto lock(getLock());
        srs = pdal::SpatialReference(reprojection->out()).getWKT();
    }

    result = makeUnique<Preview>(
            bounds,
            qi.m_pointCount,
            srs,
            qi.m_dimNames,
            scale.get(),
            metadata);

    return result;
}


Bounds Executor::transform(
        const Bounds& bounds,
        const std::vector<double>& t) const
{
    BufferState bufferState(bounds);
    UniqueStage scopedFilter(createTransformationFilter(t));
    if (!scopedFilter)
    {
        throw std::runtime_error("Could not create transformation filter");
    }

    pdal::Filter& filter(*scopedFilter->getAs<pdal::Filter*>());

    filter.setInput(bufferState.getBuffer());
    { auto lock(getLock()); filter.prepare(bufferState.getTable()); }
    filter.execute(bufferState.getTable());

    Bounds b(Bounds::expander());

    using DimId = pdal::Dimension::Id;

    for (std::size_t i(0); i < bufferState.getView().size(); ++i)
    {
        const Point p(
                bufferState.getView().getFieldAs<double>(DimId::X, i),
                bufferState.getView().getFieldAs<double>(DimId::Y, i),
                bufferState.getView().getFieldAs<double>(DimId::Z, i));

        b.grow(p);
    }

    return b;
}

std::string Executor::getSrsString(const std::string input) const
{
    auto lock(getLock());
    return pdal::SpatialReference(input).getWKT();
}

UniqueStage Executor::createReader(const std::string path) const
{
    UniqueStage result;

    const std::string driver(m_stageFactory->inferReaderDriver(path));
    if (driver.empty()) return result;

    auto lock(getLock());

    if (pdal::Reader* reader = static_cast<pdal::Reader*>(
            m_stageFactory->createStage(driver)))
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

UniqueStage Executor::createReprojectionFilter(const Reprojection& reproj) const
{
    UniqueStage result;

    if (reproj.in().empty())
    {
        throw std::runtime_error("No default SRS supplied, and none inferred");
    }

    auto lock(getLock());

    if (pdal::Filter* filter =
            static_cast<pdal::Filter*>(
                m_stageFactory->createStage("filters.reprojection")))
    {
        pdal::Options options;
        options.add(pdal::Option("in_srs", reproj.in()));
        options.add(pdal::Option("out_srs", reproj.out()));
        filter->setOptions(options);

        // Unlock before creating the ScopedStage, in case of a throw we can't
        // hold the lock during its destructor.
        lock.unlock();

        result.reset(new ScopedStage(filter, *m_stageFactory, mutex()));
    }

    return result;
}

UniqueStage Executor::createTransformationFilter(
        const std::vector<double>& matrix) const
{
    UniqueStage result;

    if (matrix.size() != 16)
    {
        throw std::runtime_error(
                "Invalid matrix length " + std::to_string(matrix.size()));
    }

    auto lock(getLock());

    if (pdal::Filter* filter =
            static_cast<pdal::Filter*>(
                m_stageFactory->createStage("filters.transformation")))
    {
        std::ostringstream ss;
        ss << std::setprecision(std::numeric_limits<double>::digits10);
        for (const double d : matrix) ss << d << " ";

        pdal::Options options;
        options.add(pdal::Option("matrix", ss.str()));
        filter->setOptions(options);

        lock.unlock();

        result = makeUnique<ScopedStage>(filter, *m_stageFactory, mutex());
    }

    return result;
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

