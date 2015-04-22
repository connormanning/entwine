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
#include <entwine/http/s3.hpp>
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
    const std::size_t httpAttempts(3);

    std::vector<char> makeEmptyPoint(const entwine::Schema& schema)
    {
        entwine::SimplePointTable table(schema);
        pdal::PointView view(table);

        view.setField(pdal::Dimension::Id::X, 0, 0);//INFINITY);
        view.setField(pdal::Dimension::Id::Y, 0, 0);//INFINITY);

        return table.data();
    }

    std::unique_ptr<pdal::Reader> createReader(
            const pdal::StageFactory& stageFactory,
            const std::string driver,
            const std::string path)
    {
        std::unique_ptr<pdal::Reader> reader(
                static_cast<pdal::Reader*>(
                    stageFactory.createStage(driver)));

        std::unique_ptr<pdal::Options> readerOptions(new pdal::Options());
        readerOptions->add(pdal::Option("filename", path));
        reader->setOptions(*readerOptions);

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
        const S3Info& s3Info,
        const std::size_t numThreads,
        const std::size_t numDimensions,
        const std::size_t baseDepth,
        const std::size_t flatDepth,
        const std::size_t diskDepth)
    : m_buildPath(buildPath)
    , m_tmpPath(tmpPath)
    , m_reprojection(reprojection.valid() ? new Reprojection(reprojection) : 0)
    , m_bbox(new BBox(bbox))
    , m_schema(new Schema(dimList))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_dimensions(numDimensions)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_stageFactory(new pdal::StageFactory())
    , m_s3(new S3(s3Info))
    , m_registry(
            new Registry(
                m_buildPath,
                *m_schema.get(),
                m_dimensions,
                baseDepth,
                flatDepth,
                diskDepth))
{
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
        const S3Info& s3Info,
        const std::size_t numThreads)
    : m_buildPath(buildPath)
    , m_tmpPath(tmpPath)
    , m_reprojection(reprojection.valid() ? new Reprojection(reprojection) : 0)
    , m_bbox()
    , m_schema()
    , m_dimensions(0)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_stageFactory(new pdal::StageFactory())
    , m_s3(new S3(s3Info))
    , m_registry()
{
    load();
}

Builder::~Builder()
{ }

void Builder::insert(const std::string& source)
{
    const Origin origin(addOrigin(source));
    std::cout << "Adding " << origin << " - " << source << std::endl;

    m_pool->add([this, origin, source]()
    {
        const std::string driver(inferDriver(source));
        const std::string localPath(fetchAndWriteFile(source, origin));

        SimplePointTable pointTable(*m_schema);

        std::unique_ptr<pdal::Reader> reader(
                createReader(
                    *m_stageFactory,
                    driver,
                    localPath));

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

        std::cout << "\tDone " << origin << " - " << source << std::endl;
        if (!fs::removeFile(localPath))
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
    m_registry->save(m_buildPath, jsonMeta["registry"]);

    // Write to disk.
    fs::writeFile(
            metaPath(),
            jsonMeta.toStyledString(),
            std::ofstream::out | std::ofstream::trunc);
}

void Builder::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        std::ifstream metaStream(metaPath());
        if (!metaStream.good())
        {
            throw std::runtime_error("Could not open " + metaPath());
        }

        reader.parse(metaStream, meta, false);
    }

    m_bbox.reset(new BBox(BBox::fromJson(meta["bbox"])));
    m_schema.reset(new Schema(Schema::fromJson(meta["schema"])));
    m_originId = m_schema->pdalLayout().findDim("Origin");
    m_dimensions = meta["dimensions"].asUInt64();
    m_numPoints = meta["numPoints"].asUInt64();
    m_numTossed = meta["numTossed"].asUInt64();
    const Json::Value& metaManifest(meta["input"]);

    for (Json::ArrayIndex i(0); i < metaManifest.size(); ++i)
    {
        m_originList.push_back(metaManifest[i].asString());
    }

    m_registry.reset(
            new Registry(
                m_buildPath,
                *m_schema.get(),
                m_dimensions,
                meta["registry"]));
}

void Builder::finalize(
        const S3Info& s3Info,
        const std::size_t base,
        const bool compress)
{
    join();

    std::unique_ptr<S3> output(new S3(s3Info));
    std::unique_ptr<std::vector<std::size_t>> ids(
            new std::vector<std::size_t>());

    const std::size_t baseEnd(Branch::calcOffset(base, m_dimensions));
    const std::size_t chunkPoints(
            baseEnd - Branch::calcOffset(base - 1, m_dimensions));

    {
        std::unique_ptr<Clipper> clipper(new Clipper(*this));

        std::vector<char> data;
        std::vector<char> emptyPoint(makeEmptyPoint(schema()));

        for (std::size_t i(0); i < baseEnd; ++i)
        {
            std::vector<char> point(getPointData(clipper.get(), i, schema()));
            if (point.size())
                data.insert(data.end(), point.begin(), point.end());
            else
                data.insert(data.end(), emptyPoint.begin(), emptyPoint.end());
        }

        auto compressed(Compression::compress(data, schema()));
        output->put(std::to_string(0), *compressed);
    }

    m_registry->finalize(*output, *m_pool, *ids, baseEnd, chunkPoints);

    {
        // Get our own metadata.
        Json::Value jsonMeta(getTreeMeta());
        jsonMeta["numIds"] = static_cast<Json::UInt64>(ids->size());
        jsonMeta["firstChunk"] = static_cast<Json::UInt64>(baseEnd);
        jsonMeta["chunkPoints"] = static_cast<Json::UInt64>(chunkPoints);
        output->put("entwine", jsonMeta.toStyledString());
    }

    Json::Value jsonIds;
    for (Json::ArrayIndex i(0); i < ids->size(); ++i)
    {
        jsonIds.append(static_cast<Json::UInt64>(ids->at(i)));
    }
    output->put("ids", jsonIds.toStyledString());
}

const BBox& Builder::getBounds() const
{
    return *m_bbox.get();
}

std::vector<std::size_t> Builder::query(
        Clipper* clipper,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    Roller roller(*m_bbox.get());
    std::vector<std::size_t> results;
    m_registry->query(roller, clipper, results, depthBegin, depthEnd);
    return results;
}

std::vector<std::size_t> Builder::query(
        Clipper* clipper,
        const BBox& bbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    Roller roller(*m_bbox.get());
    std::vector<std::size_t> results;
    m_registry->query(roller, clipper, results, bbox, depthBegin, depthEnd);
    return results;
}

std::vector<char> Builder::getPointData(
        Clipper* clipper,
        const std::size_t index,
        const Schema& reqSchema)
{
    std::vector<char> schemaPoint;
    std::vector<char> nativePoint(m_registry->getPointData(clipper, index));

    if (nativePoint.size())
    {
        schemaPoint.resize(reqSchema.pointSize());

        SinglePointTable table(schema(), nativePoint.data());
        LinkingPointView view(table);

        char* pos(schemaPoint.data());

        for (const auto& reqDim : reqSchema.dims())
        {
            view.getField(pos, reqDim.id(), reqDim.type(), 0);
            pos += reqDim.size();
        }
    }

    return schemaPoint;
}

const Schema& Builder::schema() const
{
    return *m_schema.get();
}

std::size_t Builder::numPoints() const
{
    return m_numPoints;
}

std::string Builder::path() const
{
    return m_buildPath;
}

std::string Builder::name() const
{
    std::string name;

    // TODO Temporary/hacky.
    const std::size_t pos(m_buildPath.find_last_of("/\\"));

    if (pos != std::string::npos)
    {
        name = m_buildPath.substr(pos + 1);
    }
    else
    {
        name = m_buildPath;
    }

    return name;
}

std::string Builder::metaPath() const
{
    return m_buildPath + "/meta";
}

Json::Value Builder::getTreeMeta() const
{
    Json::Value jsonMeta;
    jsonMeta["bbox"] = m_bbox->toJson();
    jsonMeta["schema"] = m_schema->toJson();
    jsonMeta["dimensions"] = static_cast<Json::UInt64>(m_dimensions);
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

Origin Builder::addOrigin(const std::string& remote)
{
    const Origin origin(m_originList.size());
    m_originList.push_back(remote);
    return origin;
}

std::string Builder::inferDriver(const std::string& remote) const
{
    const std::string driver(m_stageFactory->inferReaderDriver(remote));

    if (!driver.size())
    {
        throw std::runtime_error("No driver found - " + remote);
    }

    return driver;
}

std::string Builder::fetchAndWriteFile(
        const std::string& remote,
        const Origin origin)
{
    // Fetch remote file and write locally.
    const std::string localPath(m_tmpPath + "/" + name() + "-" +
            std::to_string(origin));

    std::size_t tries(0);
    std::unique_ptr<HttpResponse> res;

    do
    {
        res.reset(new HttpResponse(m_s3->get(remote)));
    }
    while (res->code() != 200 && ++tries < httpAttempts);

    if (res->code() != 200)
    {
        std::cout << "Couldn't fetch " + remote <<
                " - Got: " << res->code() << std::endl;
        throw std::runtime_error("Couldn't fetch " + remote);
    }

    if (!fs::writeFile(localPath, res->data(), fs::binaryTruncMode))
    {
        throw std::runtime_error("Couldn't write " + localPath);
    }

    return localPath;
}

} // namespace entwine

