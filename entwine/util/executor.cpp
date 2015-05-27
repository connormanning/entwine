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

#include <pdal/Filter.hpp>
#include <pdal/QuickInfo.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/StageWrapper.hpp>

#include <entwine/drivers/fs.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-layout.hpp>
#include <entwine/types/simple-point-table.hpp>

namespace entwine
{

namespace
{
    const std::size_t chunkBytes(65536 * 32);
}

Executor::Executor(const Schema& schema)
    : m_schema(schema)
    , m_stageFactory(new pdal::StageFactory())
    , m_fs(new FsDriver())
    , m_factoryMutex()
{ }

Executor::~Executor()
{ }

bool Executor::run(
        const std::string path,
        const Reprojection* reprojection,
        std::function<void(pdal::PointView&)> f)
{
    auto lock(getLock());
    const std::string driver(m_stageFactory->inferReaderDriver(path));
    lock.unlock();

    if (driver.empty()) return false;

    std::unique_ptr<pdal::Reader> reader(createReader(driver, path));
    if (!reader) return false;

    SimplePointTable pointTable(m_schema, chunkBytes + m_schema.pointSize());

    std::shared_ptr<pdal::Filter> sharedFilter;

    if (reprojection)
    {
        reader->setSpatialReference(
                pdal::SpatialReference(reprojection->in()));

        sharedFilter = createReprojectionFilter(*reprojection, pointTable);
    }

    pdal::Filter* filter(sharedFilter.get());

    std::size_t begin(0);

    // Set up our per-point data handler.
    reader->setReadCb(
            [&f, &pointTable, &begin, filter]
            (pdal::PointView& view, pdal::PointId index)
    {
        const std::size_t indexSpan(index - begin);

        if (
                pointTable.size() == indexSpan + 1 &&
                pointTable.data().size() > chunkBytes)
        {
            LinkingPointView link(pointTable);
            if (filter) pdal::FilterWrapper::filter(*filter, link);

            f(link);

            pointTable.clear();
            begin = index + 1;
        }
    });

    reader->prepare(pointTable);
    reader->execute(pointTable);

    // Insert leftover points.
    LinkingPointView link(pointTable);
    if (filter) pdal::FilterWrapper::filter(*filter, link);
    f(link);

    return true;
}

bool Executor::good(const std::string path) const
{
    auto lock(getLock());
    return !m_stageFactory->inferReaderDriver(path).empty();
}

std::unique_ptr<Preview> Executor::preview(
        const std::string path,
        const Reprojection* reprojection)
{
    using namespace pdal::Dimension;

    std::unique_ptr<Preview> result;

    auto lock(getLock());
    const std::string driver(m_stageFactory->inferReaderDriver(path));
    lock.unlock();

    if (!driver.empty())
    {
        std::unique_ptr<pdal::Reader> reader(createReader(driver, path));
        if (reader)
        {
            pdal::PointTable table;

            auto layout(table.layout());
            layout->registerDim(Id::X);
            layout->registerDim(Id::Y);
            layout->registerDim(Id::Z);

            const pdal::QuickInfo quick(reader->preview());

            if (quick.valid())
            {
                BBox bbox(
                        Point(quick.m_bounds.minx, quick.m_bounds.miny),
                        Point(quick.m_bounds.maxx, quick.m_bounds.maxy));

                if (reprojection)
                {
                    auto filter(
                            createReprojectionFilter(*reprojection, table));

                    pdal::PointView view(table);

                    view.setField(Id::X, 0, bbox.min().x);
                    view.setField(Id::Y, 0, bbox.min().y);
                    view.setField(Id::X, 1, bbox.max().x);
                    view.setField(Id::Y, 1, bbox.max().y);

                    pdal::FilterWrapper::filter(*filter, view);

                    bbox = BBox(
                            Point(
                                view.getFieldAs<double>(Id::X, 0),
                                view.getFieldAs<double>(Id::Y, 0)),
                            Point(
                                view.getFieldAs<double>(Id::X, 1),
                                view.getFieldAs<double>(Id::Y, 1)));
                }

                result.reset(new Preview(bbox, quick.m_pointCount));
            }
        }
    }

    return result;
}

std::unique_ptr<pdal::Reader> Executor::createReader(
        const std::string driver,
        const std::string path) const
{
    std::unique_ptr<pdal::Reader> reader;

    if (driver.size())
    {
        auto lock(getLock());
        reader.reset(
                static_cast<pdal::Reader*>(
                    m_stageFactory->createStage(driver)));
        lock.unlock();

        std::unique_ptr<pdal::Options> readerOptions(new pdal::Options());
        readerOptions->add(pdal::Option("filename", path));
        reader->setOptions(*readerOptions);
    }
    else
    {
        // TODO Try executing as pipeline.
    }

    return reader;
}

std::shared_ptr<pdal::Filter> Executor::createReprojectionFilter(
        const Reprojection& reproj,
        pdal::BasePointTable& pointTable) const
{
    auto lock(getLock());
    std::shared_ptr<pdal::Filter> filter(
            static_cast<pdal::Filter*>(
                m_stageFactory->createStage("filters.reprojection")));
    lock.unlock();

    std::unique_ptr<pdal::Options> reprojOptions(new pdal::Options());
    reprojOptions->add(
            pdal::Option(
                "in_srs",
                pdal::SpatialReference(reproj.in())));
    reprojOptions->add(
            pdal::Option(
                "out_srs",
                pdal::SpatialReference(reproj.out())));

    pdal::FilterWrapper::initialize(filter, pointTable);
    pdal::FilterWrapper::processOptions(*filter, *reprojOptions);
    pdal::FilterWrapper::ready(*filter, pointTable);

    return filter;
}

std::unique_lock<std::mutex> Executor::getLock() const
{
    return std::unique_lock<std::mutex>(m_factoryMutex);
}

} // namespace entwine

