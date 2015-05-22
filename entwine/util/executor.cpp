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
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/StageWrapper.hpp>

#include <entwine/drivers/fs.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
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

