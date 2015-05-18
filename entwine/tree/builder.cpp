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
#include <pdal/PointView.hpp>
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
#include <entwine/util/executor.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

Builder::Builder(
        const std::string buildPath,
        const std::string tmpPath,
        const Reprojection* reprojection,
        const BBox* bbox,
        const DimList& dimList,
        const std::size_t numThreads,
        const std::size_t numDimensions,
        const std::size_t chunkPoints,
        const std::size_t baseDepth,
        const std::size_t flatDepth,
        const std::size_t diskDepth,
        std::shared_ptr<Arbiter> arbiter)
    : m_reprojection(reprojection ? new Reprojection(*reprojection) : 0)
    , m_bbox(bbox ? new BBox(*bbox) : 0)
    , m_schema(new Schema(dimList))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_dimensions(numDimensions)
    , m_chunkPoints(chunkPoints)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_manifest()
    , m_pool(new Pool(numThreads))
    , m_executor(new Executor(*m_schema))
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_buildSource(m_arbiter->getSource(buildPath))
    , m_tmpSource  (m_arbiter->getSource(tmpPath))
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
        const Reprojection* reprojection,
        const std::size_t numThreads,
        std::shared_ptr<Arbiter> arbiter)
    : m_reprojection(reprojection ? new Reprojection(*reprojection) : 0)
    , m_bbox()
    , m_schema()
    , m_dimensions(0)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_manifest()
    , m_pool(new Pool(numThreads))
    , m_executor()
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_buildSource(m_arbiter->getSource(buildPath))
    , m_tmpSource  (m_arbiter->getSource(tmpPath))
    , m_registry()
{
    prep();
    load();
}

Builder::~Builder()
{ }

bool Builder::insert(const std::string path)
{
    if (!m_executor->good(path))
    {
        m_manifest.addOmission(path);
        return false;
    }

    const Origin origin(m_manifest.addOrigin(path));

    if (origin == Manifest::invalidOrigin()) return false;  // Already inserted.

    if (!m_bbox && origin == 0)
    {
        inferBBox(path);
    }

    std::cout << "Adding " << origin << " - " << path << std::endl;

    m_pool->add([this, origin, path]()
    {
        const bool isRemote(m_arbiter->getSource(path).isRemote());
        const std::string localPath(localize(path, origin));

        std::unique_ptr<Clipper> clipperPtr(new Clipper(*this));
        Clipper* clipper(clipperPtr.get());

        auto inserter([this, origin, clipper](pdal::PointView& view)->void
        {
            insert(view, origin, clipper);
        });

        try
        {
            if (!m_executor->run(localPath, m_reprojection.get(), inserter))
            {
                m_manifest.addError(origin);
            }
        }
        catch (std::runtime_error e)
        {
            std::cout << "During " << path << ": " << e.what() << std::endl;
            m_manifest.addError(origin);
        }
        catch (...)
        {
            std::cout << "Caught unknown error during " << path << std::endl;
            m_manifest.addError(origin);
        }

        std::cout << "\tDone " << origin << " - " << path << std::endl;

        if (isRemote && !fs::removeFile(localPath))
        {
            std::cout << "Couldn't delete " << localPath << std::endl;
            throw std::runtime_error("Couldn't delete tmp file");
        }
    });

    return true;
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

void Builder::inferBBox(const std::string path)
{
    std::cout << "Inferring bounds from " << path << "..." << std::endl;

    // Use BBox::set() to avoid malformed BBox warning.
    BBox bbox;
    bbox.set(
            Point(
                std::numeric_limits<double>::max(),
                std::numeric_limits<double>::max()),
            Point(
                std::numeric_limits<double>::lowest(),
                std::numeric_limits<double>::lowest()));

    const std::string localPath(localize(path, 0));

    auto bounder([this, &bbox](pdal::PointView& view)->void
    {
        for (std::size_t i = 0; i < view.size(); ++i)
        {
            bbox.grow(
                    Point(
                        view.getFieldAs<double>(pdal::Dimension::Id::X, i),
                        view.getFieldAs<double>(pdal::Dimension::Id::Y, i)));
        }
    });

    if (!m_executor->run(localPath, m_reprojection.get(), bounder))
    {
        throw std::runtime_error("Error inferring bounds");
    }

    m_bbox.reset(
            new BBox(
                Point(std::floor(bbox.min().x), std::floor(bbox.min().y)),
                Point(std::ceil(bbox.max().x),  std::ceil(bbox.max().y))));

    std::cout << "Got: " << m_bbox->toJson().toStyledString() << std::endl;
}

std::string Builder::localize(const std::string path, const Origin origin)
{
    Source source(m_arbiter->getSource(path));
    std::string localPath(source.path());

    if (source.isRemote())
    {
        const std::string subpath(name() + "-" + std::to_string(origin));

        localPath = m_tmpSource.resolve(subpath);
        m_tmpSource.put(subpath, source.getRoot());
    }

    return localPath;
}

void Builder::clip(Clipper* clipper, std::size_t index)
{
    m_registry->clip(clipper, index);
}

void Builder::join()
{
    m_pool->join();
}

void Builder::save()
{
    // Ensure constant state, waiting for all worker threads to complete.
    join();

    // Get our own metadata and the registry's - then serialize.
    Json::Value jsonMeta(saveProps());
    m_registry->save(jsonMeta["registry"]);
    m_buildSource.put("meta", jsonMeta.toStyledString());

    // Re-allow inserts.
    m_pool->go();
}

void Builder::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        const std::string data(m_buildSource.getAsString("meta"));
        reader.parse(data, meta, false);
    }

    loadProps(meta);

    m_originId = m_schema->pdalLayout().findDim("Origin");

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
        Json::Value jsonMeta(saveProps());
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

Json::Value Builder::saveProps() const
{
    // Reprojection info is intentionally omitted here, for now.  This would
    // allow a saved build that was reprojected from A->B to be continued with
    // a different set of files needing projection from X->Y, without requiring
    // this to be set per-file in the configuration.

    Json::Value props;

    props["bbox"] = m_bbox->toJson();
    props["schema"] = m_schema->toJson();
    props["dimensions"] = static_cast<Json::UInt64>(m_dimensions);
    props["chunkPoints"] = static_cast<Json::UInt64>(m_chunkPoints);
    props["numPoints"] = static_cast<Json::UInt64>(m_numPoints);
    props["numTossed"] = static_cast<Json::UInt64>(m_numTossed);
    props["manifest"] = m_manifest.getJson();

    return props;
}

void Builder::loadProps(const Json::Value& props)
{
    m_bbox.reset(new BBox(BBox::fromJson(props["bbox"])));
    m_schema.reset(new Schema(Schema::fromJson(props["schema"])));
    m_executor.reset(new Executor(*m_schema));
    m_dimensions = props["dimensions"].asUInt64();
    m_chunkPoints = props["chunkPoints"].asUInt64();
    m_numPoints = props["numPoints"].asUInt64();
    m_numTossed = props["numTossed"].asUInt64();
    m_manifest = Manifest(props["manifest"]);
}

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

} // namespace entwine

