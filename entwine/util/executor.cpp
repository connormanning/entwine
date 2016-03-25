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

#include <pdal/BufferReader.hpp>
#include <pdal/Filter.hpp>
#include <pdal/GlobalEnvironment.hpp>
#include <pdal/QuickInfo.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>

#include <entwine/types/bbox.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

namespace
{
    Reprojection srsFoundOrDefault(
            const pdal::SpatialReference& found,
            const Reprojection& given)
    {
        if (given.hammer() || found.empty()) return given;
        else return Reprojection(found.getWKT(), given.out());
    }

    const auto env(([]()
    {
        pdal::GlobalEnvironment::startup();
        return true;
    })());

    const double hi(std::numeric_limits<double>::max());
    const double lo(std::numeric_limits<double>::lowest());

    class BufferState
    {
    public:
        BufferState(const BBox& bbox)
            : m_table()
            , m_view(new pdal::PointView(m_table))
            , m_buffer()
        {
            namespace DimId = pdal::Dimension::Id;

            auto layout(m_table.layout());
            layout->registerDim(DimId::X);
            layout->registerDim(DimId::Y);
            layout->registerDim(DimId::Z);
            layout->finalize();

            m_view->setField(DimId::X, 0, bbox.min().x);
            m_view->setField(DimId::Y, 0, bbox.min().y);
            m_view->setField(DimId::Z, 0, bbox.min().z);

            m_view->setField(DimId::X, 1, bbox.max().x);
            m_view->setField(DimId::Y, 1, bbox.max().y);
            m_view->setField(DimId::Z, 1, bbox.max().z);

            m_view->setField(DimId::X, 2, bbox.min().x);
            m_view->setField(DimId::Y, 2, bbox.max().y);
            m_view->setField(DimId::Z, 2, bbox.min().z);

            m_view->setField(DimId::X, 3, bbox.max().x);
            m_view->setField(DimId::Y, 3, bbox.min().y);
            m_view->setField(DimId::Z, 3, bbox.max().z);

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

Executor::Executor(bool is3d)
    : m_is3d(is3d)
    , m_stageFactory(new pdal::StageFactory())
    , m_factoryMutex()
{ }

Executor::~Executor()
{ }

bool Executor::run(
        PooledPointTable& table,
        const std::string path,
        const Reprojection* reprojection)
{
    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return false;

    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    reader->prepare(table);

    pdal::Stage* executor(reader);

    UniqueStage scopedFilter;

    if (reprojection)
    {
        const auto srs(
                srsFoundOrDefault(
                    reader->getSpatialReference(), *reprojection));

        scopedFilter = createReprojectionFilter(srs);
        if (!scopedFilter) return false;

        pdal::Filter* filter(scopedFilter->getAs<pdal::Filter*>());
        filter->setInput(*reader);

        executor = filter;
    }

    executor->prepare(table);
    executor->execute(table);

    return true;
}

bool Executor::good(const std::string path) const
{
    return !m_stageFactory->inferReaderDriver(path).empty();
}

std::unique_ptr<Preview> Executor::preview(
        const std::string path,
        const Reprojection* reprojection)
{
    std::unique_ptr<Preview> result;

    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return result;

    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    const pdal::QuickInfo qi(reader->preview());
    if (!qi.valid() || qi.m_bounds.empty()) return result;

    BBox bbox(
            Point(qi.m_bounds.minx, qi.m_bounds.miny, qi.m_bounds.minz),
            Point(qi.m_bounds.maxx, qi.m_bounds.maxy, qi.m_bounds.maxz),
            m_is3d);

    std::string srs;

    if (!reprojection)
    {
        srs = qi.m_srs.getWKT();
    }
    else
    {
        namespace DimId = pdal::Dimension::Id;

        BufferState bufferState(bbox);

        const auto selectedSrs(
                srsFoundOrDefault(qi.m_srs, *reprojection));

        UniqueStage scopedFilter(createReprojectionFilter(selectedSrs));
        if (!scopedFilter) return result;

        pdal::Filter* filter(scopedFilter->getAs<pdal::Filter*>());

        filter->setInput(bufferState.getBuffer());
        filter->prepare(bufferState.getTable());
        filter->execute(bufferState.getTable());

        Point min(hi, hi, hi);
        Point max(lo, lo, lo);

        for (std::size_t i(0); i < bufferState.getView().size(); ++i)
        {
            const Point p(
                    bufferState.getView().getFieldAs<double>(DimId::X, i),
                    bufferState.getView().getFieldAs<double>(DimId::Y, i),
                    bufferState.getView().getFieldAs<double>(DimId::Z, i));

            min.x = std::min(min.x, p.x);
            min.y = std::min(min.y, p.y);
            min.z = std::min(min.z, p.z);

            max.x = std::max(max.x, p.x);
            max.y = std::max(max.y, p.y);
            max.z = std::max(max.z, p.z);
        }

        bbox = BBox(min, max, m_is3d);
        srs = pdal::SpatialReference(reprojection->out()).getWKT();
    }

    result.reset(new Preview(bbox, qi.m_pointCount, srs, qi.m_dimNames));
    return result;
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

        result.reset(new ScopedStage(reader, *m_stageFactory, m_factoryMutex));
    }

    return result;
}

UniqueStage Executor::createReprojectionFilter(
        const Reprojection& reproj) const
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

        result.reset(new ScopedStage(filter, *m_stageFactory, m_factoryMutex));
    }

    return result;
}

std::unique_lock<std::mutex> Executor::getLock() const
{
    return std::unique_lock<std::mutex>(m_factoryMutex);
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

