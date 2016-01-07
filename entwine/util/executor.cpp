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
#include <pdal/QuickInfo.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/StageWrapper.hpp>

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
        if (found.empty()) return given;
        else return Reprojection(found.getWKT(), given.out());
    }
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
    auto lock(getLock());
    const std::string driver(m_stageFactory->inferReaderDriver(path));

    if (driver.empty()) return false;

    std::unique_ptr<pdal::Reader> reader(createReader(driver, path));
    if (!reader) return false;
    reader->prepare(table);

    pdal::Stage* executor(reader.get());

    std::unique_ptr<pdal::Filter> filter;

    if (reprojection)
    {
        filter =
            createReprojectionFilter(
                srsFoundOrDefault(reader->getSpatialReference(), *reprojection),
                table);

        if (!filter) return false;

        filter->setInput(*reader);
        executor = filter.get();
    }

    executor->prepare(table);
    lock.unlock();

    executor->execute(table);

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
    using namespace pdal;

    std::unique_ptr<Preview> result;

    auto lock(getLock());
    const std::string driver(m_stageFactory->inferReaderDriver(path));

    if (!driver.empty())
    {
        std::unique_ptr<pdal::Reader> reader(createReader(driver, path));
        if (reader)
        {
            const pdal::QuickInfo quick(reader->preview());

            if (quick.valid())
            {
                std::string srs;

                BBox bbox(
                        Point(
                            quick.m_bounds.minx,
                            quick.m_bounds.miny,
                            quick.m_bounds.minz),
                        Point(
                            quick.m_bounds.maxx,
                            quick.m_bounds.maxy,
                            quick.m_bounds.maxz),
                        m_is3d);

                if (reprojection)
                {
                    pdal::PointTable table;
                    auto layout(table.layout());
                    layout->registerDim(Dimension::Id::X);
                    layout->registerDim(Dimension::Id::Y);
                    layout->registerDim(Dimension::Id::Z);
                    layout->finalize();

                    pdal::PointViewPtr view(new pdal::PointView(table));
                    view->setField(Dimension::Id::X, 0, bbox.min().x);
                    view->setField(Dimension::Id::Y, 0, bbox.min().y);
                    view->setField(Dimension::Id::Z, 0, bbox.min().z);
                    view->setField(Dimension::Id::X, 1, bbox.max().x);
                    view->setField(Dimension::Id::Y, 1, bbox.max().y);
                    view->setField(Dimension::Id::Z, 1, bbox.max().z);

                    pdal::BufferReader buffer;
                    buffer.addView(view);

                    std::unique_ptr<pdal::Filter> filter(
                            createReprojectionFilter(
                                srsFoundOrDefault(quick.m_srs, *reprojection),
                                table));

                    filter->setInput(buffer);

                    filter->prepare(table);
                    filter->execute(table);

                    bbox = BBox(
                            Point(
                                view->getFieldAs<double>(Dimension::Id::X, 0),
                                view->getFieldAs<double>(Dimension::Id::Y, 0),
                                view->getFieldAs<double>(Dimension::Id::Z, 0)),
                            Point(
                                view->getFieldAs<double>(Dimension::Id::X, 1),
                                view->getFieldAs<double>(Dimension::Id::Y, 1),
                                view->getFieldAs<double>(Dimension::Id::Z, 1)),
                            m_is3d);

                    srs = pdal::SpatialReference(reprojection->out()).getWKT();
                }
                else
                {
                    srs = quick.m_srs.getWKT();
                }

                result.reset(
                        new Preview(
                            bbox,
                            quick.m_pointCount,
                            srs,
                            quick.m_dimNames));
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
        reader.reset(
                static_cast<pdal::Reader*>(
                    m_stageFactory->createStage(driver)));

        pdal::Options options;
        options.add(pdal::Option("filename", path));
        reader->setOptions(options);
    }
    else
    {
        // TODO Try executing as pipeline.
    }

    return reader;
}

std::unique_ptr<pdal::Filter> Executor::createReprojectionFilter(
        const Reprojection& reproj,
        pdal::BasePointTable& pointTable) const
{
    if (reproj.in().empty())
    {
        throw std::runtime_error("No default SRS supplied, and none inferred");
    }

    std::unique_ptr<pdal::Filter> filter(
            static_cast<pdal::Filter*>(
                m_stageFactory->createStage("filters.reprojection")));

    pdal::Options options;
    options.add(
            pdal::Option(
                "in_srs",
                pdal::SpatialReference(reproj.in())));
    options.add(
            pdal::Option(
                "out_srs",
                pdal::SpatialReference(reproj.out())));
    filter->setOptions(options);

    return filter;
}

std::unique_lock<std::mutex> Executor::getLock() const
{
    return std::unique_lock<std::mutex>(m_factoryMutex);
}

} // namespace entwine

