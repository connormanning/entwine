/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/builder.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/Filter.hpp>
#include <pdal/PointView.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/StageWrapper.hpp>
#include <pdal/Utils.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/drivers/arbiter.hpp>
#include <entwine/tree/branch.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/fs.hpp>

namespace
{
    std::unique_ptr<pdal::Reader> createReader(
            const pdal::StageFactory& stageFactory,
            const std::string driver,
            const std::string path)
    {
        std::unique_ptr<pdal::Reader> reader;

        if (driver.size())
        {
            reader.reset(
                    static_cast<pdal::Reader*>(
                        stageFactory.createStage(driver)));

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

    std::shared_ptr<pdal::Filter> createReprojectionFilter(
            const pdal::StageFactory& stageFactory,
            const entwine::Reprojection& reproj,
            pdal::BasePointTable& pointTable)
    {
        std::shared_ptr<pdal::Filter> filter(
                static_cast<pdal::Filter*>(
                    stageFactory.createStage("filters.reprojection")));

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
}

namespace entwine
{

Builder::Builder(
        const std::string buildPath,
        const std::string tmpPath,
        const Reprojection& reprojection,
        const BBox& bbox,
        const DimList& dimList,
        const std::size_t numThreads,
        const std::size_t numDimensions,
        const std::size_t chunkPoints,
        const std::size_t baseDepth,
        const std::size_t flatDepth,
        const std::size_t diskDepth,
        std::shared_ptr<Arbiter> arbiter)
    : m_reprojection(reprojection.valid() ? new Reprojection(reprojection) : 0)
    , m_bbox(new BBox(bbox))
    , m_schema(new Schema(dimList))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_dimensions(numDimensions)
    , m_chunkPoints(chunkPoints)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_buildSource(m_arbiter->getSource(buildPath))
    , m_tmpSource  (m_arbiter->getSource(tmpPath))
    , m_stageFactory(new pdal::StageFactory())
    , m_registry(
            new Registry(
                m_buildSource,
                *m_schema.get(),
                m_dimensions,
                m_chunkPoints,
                baseDepth,
                flatDepth,
                diskDepth))
{
    prep();

    if (m_dimensions != 2)
    {
        // TODO
        throw std::runtime_error("TODO - Only 2 dimensions so far");
    }
}

Builder::Builder(
        const std::string buildPath,
        const std::string tmpPath,
        const Reprojection& reprojection,
        const std::size_t numThreads,
        std::shared_ptr<Arbiter> arbiter)
    : m_reprojection(reprojection.valid() ? new Reprojection(reprojection) : 0)
    , m_bbox()
    , m_schema()
    , m_dimensions(0)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_buildSource(m_arbiter->getSource(buildPath))
    , m_tmpSource  (m_arbiter->getSource(tmpPath))
    , m_stageFactory(new pdal::StageFactory())
    , m_registry()
{
    prep();
    load();
}


Builder::~Builder()
{ }

void Builder::prep()
{
    if (m_tmpSource.isRemote())
    {
        throw std::runtime_error("Tmp path must be local");
    }

    if (!fs::mkdirp(m_tmpSource.path()))
    {
        throw std::runtime_error("Couldn't create tmp directory");
    }

    if (!m_buildSource.isRemote() && !fs::mkdirp(m_buildSource.path()))
    {
        throw std::runtime_error("Couldn't create local build directory");
    }
}

void Builder::insert(const std::string path)
{
    // TODO Check for duplicates to allow continuation without modding config.
    const Origin origin(addOrigin(path));
    std::cout << "Adding " << origin << " - " << path << std::endl;

    m_pool->add([this, origin, path]()
    {
        Source source(m_arbiter->getSource(path));
        const bool isRemote(source.isRemote());
        std::string localPath(source.path());

        if (isRemote)
        {
            const std::string subpath(name() + "-" + std::to_string(origin));

            localPath = m_tmpSource.resolve(subpath);
            m_tmpSource.put(subpath, source.getRoot());
        }

        SimplePointTable pointTable(*m_schema);

        std::unique_ptr<pdal::Reader> reader(
                createReader(
                    *m_stageFactory,
                    inferPdalDriver(path),
                    localPath));

        if (reader)
        {
            // TODO Watch for errors during insertion.  If any occur, mark as
            // a partial or failed insertion.

            std::shared_ptr<pdal::Filter> sharedFilter;

            if (m_reprojection)
            {
                reader->setSpatialReference(
                        pdal::SpatialReference(m_reprojection->in()));

                sharedFilter = createReprojectionFilter(
                        *m_stageFactory,
                        *m_reprojection,
                        pointTable);
            }

            pdal::Filter* filter(sharedFilter.get());

            // TODO Should pass to insert as reference, not ptr.
            std::unique_ptr<Clipper> clipper(new Clipper(*this));
            Clipper* clipperPtr(clipper.get());

            // Set up our per-point data handler.
            reader->setReadCb(
                    [this, &pointTable, filter, origin, clipperPtr]
                    (pdal::PointView& view, pdal::PointId index)
            {
                // TODO This won't work for dimension-oriented readers that
                // have partially written points after the given PointId.
                LinkingPointView link(pointTable);

                if (filter) pdal::FilterWrapper::filter(*filter, link);

                insert(link, origin, clipperPtr);
                pointTable.clear();
            });

            reader->prepare(pointTable);
            reader->execute(pointTable);

        }
        else
        {
            // TODO Mark this file as not inserted.  This is not exceptional,
            // especially if we're inserting from a globbed path which may
            // contain non-point-cloud files.
        }

        std::cout << "\tDone " << origin << " - " << path << std::endl;
        if (isRemote && !fs::removeFile(localPath))
        {
            std::cout << "Couldn't delete " << localPath << std::endl;
            throw std::runtime_error("Couldn't delete tmp file");
        }
    });
}

void Builder::insert(
        pdal::PointView& pointView,
        Origin origin,
        Clipper* clipper)
{
    Point point;

    for (std::size_t i = 0; i < pointView.size(); ++i)
    {
        point.x = pointView.getFieldAs<double>(pdal::Dimension::Id::X, i);
        point.y = pointView.getFieldAs<double>(pdal::Dimension::Id::Y, i);

        if (m_bbox->contains(point))
        {
            Roller roller(*m_bbox.get());

            pointView.setField(m_originId, i, origin);

            PointInfo* pointInfo(
                    new PointInfo(
                        new Point(point),
                        pointView.getPoint(i),
                        m_schema->pointSize()));

            if (m_registry->addPoint(&pointInfo, roller, clipper))
            {
                ++m_numPoints;
            }
            else
            {
                ++m_numTossed;
            }
        }
        else
        {
            ++m_numTossed;
        }
    }
}

void Builder::join()
{
    m_pool->join();
}

void Builder::clip(Clipper* clipper, std::size_t index)
{
    m_registry->clip(clipper, index);
}

void Builder::save()
{
    // Ensure static state.
    join();

    // Get our own metadata.
    Json::Value jsonMeta(getTreeMeta());

    // Add the registry's metadata.
    m_registry->save(jsonMeta["registry"]);

    // Write to disk.
    m_buildSource.put("meta", jsonMeta.toStyledString());
}

Json::Value Builder::getTreeMeta() const
{
    Json::Value jsonMeta;
    jsonMeta["bbox"] = m_bbox->toJson();
    jsonMeta["schema"] = m_schema->toJson();
    jsonMeta["dimensions"] = static_cast<Json::UInt64>(m_dimensions);
    jsonMeta["chunkPoints"] = static_cast<Json::UInt64>(m_chunkPoints);
    jsonMeta["numPoints"] = static_cast<Json::UInt64>(m_numPoints);
    jsonMeta["numTossed"] = static_cast<Json::UInt64>(m_numTossed);

    // Add origin list to meta.
    Json::Value& jsonManifest(jsonMeta["input"]);
    for (Json::ArrayIndex i(0); i < m_originList.size(); ++i)
    {
        jsonManifest.append(m_originList[i]);
    }

    return jsonMeta;
}

void Builder::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        const std::string data(m_buildSource.getAsString("meta"));
        reader.parse(data, meta, false);
    }

    m_originId = m_schema->pdalLayout().findDim("Origin");

    m_bbox.reset(new BBox(BBox::fromJson(meta["bbox"])));
    m_schema.reset(new Schema(Schema::fromJson(meta["schema"])));
    m_dimensions = meta["dimensions"].asUInt64();
    m_chunkPoints = meta["chunkPoints"].asUInt64();
    m_numPoints = meta["numPoints"].asUInt64();
    m_numTossed = meta["numTossed"].asUInt64();

    const Json::Value& metaManifest(meta["input"]);

    for (Json::ArrayIndex i(0); i < metaManifest.size(); ++i)
    {
        m_originList.push_back(metaManifest[i].asString());
    }

    m_registry.reset(
            new Registry(
                m_buildSource,
                *m_schema.get(),
                m_dimensions,
                m_chunkPoints,
                meta["registry"]));
}

void Builder::finalize(
        const std::string path,
        const std::size_t chunkPoints,
        const std::size_t base,
        const bool compress)
{
    join();

    Source outputSource(m_arbiter->getSource(path));
    if (!outputSource.isRemote() && !fs::mkdirp(outputSource.path()))
    {
        throw std::runtime_error("Could not create " + outputSource.path());
    }

    std::unique_ptr<std::vector<std::size_t>> ids(
            new std::vector<std::size_t>());

    const std::size_t baseEnd(Branch::calcOffset(base, m_dimensions));

    m_registry->finalize(outputSource, *m_pool, *ids, baseEnd, chunkPoints);
    m_pool->join();

    {
        // Get our own metadata.
        Json::Value jsonMeta(getTreeMeta());
        jsonMeta["numIds"] = static_cast<Json::UInt64>(ids->size());
        jsonMeta["firstChunk"] = static_cast<Json::UInt64>(baseEnd);
        jsonMeta["chunkPoints"] = static_cast<Json::UInt64>(chunkPoints);
        outputSource.put("entwine", jsonMeta.toStyledString());
    }

    Json::Value jsonIds;
    for (Json::ArrayIndex i(0); i < ids->size(); ++i)
    {
        jsonIds.append(static_cast<Json::UInt64>(ids->at(i)));
    }

    outputSource.put("ids", jsonIds.toStyledString());
}

const BBox& Builder::getBounds() const
{
    return *m_bbox.get();
}

const Schema& Builder::schema() const
{
    return *m_schema.get();
}

std::size_t Builder::numPoints() const
{
    return m_numPoints;
}

std::string Builder::name() const
{
    std::string name(m_buildSource.path());

    // TODO Temporary/hacky.
    const std::size_t pos(name.find_last_of("/\\"));

    if (pos != std::string::npos)
    {
        name = name.substr(pos + 1);
    }

    return name;
}

Origin Builder::addOrigin(const std::string& remote)
{
    const Origin origin(m_originList.size());
    m_originList.push_back(remote);
    return origin;
}

std::string Builder::inferPdalDriver(const std::string& path) const
{
    const std::string driver(m_stageFactory->inferReaderDriver(path));

    if (!driver.size())
    {
        throw std::runtime_error("No driver found - " + path);
    }

    return driver;
}

} // namespace entwine

