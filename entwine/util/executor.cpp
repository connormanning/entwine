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
    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return nullptr;

    auto result(makeUnique<Preview>());
    auto& p(*result);

    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    const pdal::QuickInfo qi(([this, reader]()
    {
        auto lock(getLock());
        return reader->preview();
    })());

    if (qi.valid())
    {
        if (!qi.m_bounds.empty())
        {
            p.bounds = Bounds(
                    qi.m_bounds.minx, qi.m_bounds.miny, qi.m_bounds.minz,
                    qi.m_bounds.maxx, qi.m_bounds.maxy, qi.m_bounds.maxz);
        }

        { auto lock(getLock()); p.srs = qi.m_srs.getWKT(); }
        p.numPoints = qi.m_pointCount;
        p.dimNames = qi.m_dimNames;

        if (const auto las = dynamic_cast<pdal::LasReader*>(reader))
        {
            const auto& h(las->header());
            p.scale = makeUnique<Scale>(h.scaleX(), h.scaleY(), h.scaleZ());
        }

        p.metadata = ([this, reader]()
        {
            auto lock(getLock());
            const auto s(pdal::Utils::toJSON(reader->getMetadata()));
            try { return parse(s); }
            catch (...) { return Json::Value(s); }
        })();
    }
    else
    {
        using D = pdal::Dimension::Id;
        const Schema xyzSchema({ { D::X }, { D::Y }, { D::Z } });
        PointPool pointPool(xyzSchema);

        p.bounds = Bounds::expander();

        // We'll pick up the number of points and bounds here.
        auto counter([&p](Cell::PooledStack stack)
        {
            p.numPoints += stack.size();
            for (const auto& cell : stack) p.bounds.grow(cell.point());
            return stack;
        });

        PooledPointTable table(pointPool, counter, invalidOrigin);

        if (Executor::get().run(table, path))
        {
            // And we'll pick up added dimensions here.
            for (const auto& d : xyzSchema.fixedLayout().added())
            {
                p.dimNames.push_back(d.first);
            }
        }
    }

    // Now we have all of our info in native format.  If a reprojection has
    // been set, then we'll need to transform our bounds and SRS values.
    if (reprojection)
    {
        using DimId = pdal::Dimension::Id;

        BufferState bufferState(p.bounds);

        const auto selectedSrs(
                srsFoundOrDefault(qi.m_srs, *reprojection));

        UniqueStage scopedFilter(createReprojectionFilter(selectedSrs));
        if (!scopedFilter) return result;

        pdal::Filter& filter(*scopedFilter->getAs<pdal::Filter*>());

        filter.setInput(bufferState.getBuffer());
        { auto lock(getLock()); filter.prepare(bufferState.getTable()); }
        filter.execute(bufferState.getTable());

        p.bounds = Bounds::expander();
        for (std::size_t i(0); i < bufferState.getView().size(); ++i)
        {
            const Point point(
                    bufferState.getView().getFieldAs<double>(DimId::X, i),
                    bufferState.getView().getFieldAs<double>(DimId::Y, i),
                    bufferState.getView().getFieldAs<double>(DimId::Z, i));

            p.bounds.grow(point);
        }

        auto lock(getLock());
        p.srs = pdal::SpatialReference(reprojection->out()).getWKT();
    }

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

