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
#include <entwine/tree/roller.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/tree/clipper.hpp>
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
        const std::string outPath,
        const std::string tmpPath,
        const Reprojection* reprojection,
        const BBox* bbox,
        const DimList& dimList,
        const std::size_t numThreads,
        const Structure& structure,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox(bbox ? new BBox(*bbox) : 0)
    , m_schema(new Schema(dimList))
    , m_structure(new Structure(structure))
    , m_reprojection(reprojection ? new Reprojection(*reprojection) : 0)
    , m_manifest(new Manifest())
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_executor(new Executor(*m_schema))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_outSource(m_arbiter->getSource(outPath))
    , m_tmpSource(m_arbiter->getSource(tmpPath))
    , m_registry(
            new Registry(
                m_outSource,
                *m_schema,
                *m_structure))
{
    prep();
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t numThreads,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_numPoints(0)
    , m_numTossed(0)
    , m_pool(new Pool(numThreads))
    , m_executor()
    , m_arbiter(arbiter ? arbiter : std::make_shared<Arbiter>(Arbiter()))
    , m_outSource(m_arbiter->getSource(outPath))
    , m_tmpSource(m_arbiter->getSource(tmpPath))
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
        m_manifest->addOmission(path);
        return false;
    }

    const Origin origin(m_manifest->addOrigin(path));

    if (origin == Manifest::invalidOrigin()) return false;  // Already inserted.

    if (!m_bbox && origin == 0)
    {
        inferBBox(path);
    }

    std::cout << "Adding " << origin << " - " << path << std::endl;

    m_pool->add([this, origin, path]()
    {
        try
        {
            const bool isRemote(m_arbiter->getSource(path).isRemote());
            const std::string localPath(localize(path, origin));

            std::unique_ptr<Clipper> clipperPtr(new Clipper(*this));
            Clipper* clipper(clipperPtr.get());

            auto inserter([this, origin, clipper](pdal::PointView& view)->void
            {
                insert(view, origin, clipper);
            });

            if (!m_executor->run(localPath, m_reprojection.get(), inserter))
            {
                m_manifest->addError(origin);
            }

            std::cout << "\tDone " << origin << " - " << path << std::endl;

            if (isRemote && !fs::removeFile(localPath))
            {
                std::cout << "Couldn't delete " << localPath << std::endl;
                throw std::runtime_error("Couldn't delete tmp file");
            }
        }
        catch (std::runtime_error e)
        {
            std::cout << "During " << path << ": " << e.what() << std::endl;
            m_manifest->addError(origin);
        }
        catch (...)
        {
            std::cout << "Caught unknown error during " << path << std::endl;
            m_manifest->addError(origin);
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
        std::size_t dot(path.find_last_of("."));

        if (dot != std::string::npos)
        {
            const std::string subpath(
                    name() + "-" + std::to_string(origin) + path.substr(dot));

            localPath = m_tmpSource.resolve(subpath);
            m_tmpSource.put(subpath, source.getRoot());
        }
        else
        {
            throw std::runtime_error("Bad extension on: " + path);
        }
    }

    return localPath;
}

void Builder::clip(std::size_t index, Clipper* clipper)
{
    m_registry->clip(index, clipper);
}

void Builder::join()
{
    m_pool->join();
}

void Builder::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        const std::string data(m_outSource.getAsString("entwine"));
        reader.parse(data, meta, false);
    }

    loadProps(meta);

    m_executor.reset(new Executor(*m_schema));
    m_originId = m_schema->pdalLayout().findDim("Origin");

    m_registry.reset(
            new Registry(
                m_outSource,
                *m_schema,
                *m_structure,
                meta));
}

void Builder::save()
{
    // Ensure constant state, waiting for all worker threads to complete.
    join();

    // Get our own metadata and the registry's - then serialize.
    Json::Value jsonMeta(saveProps());
    m_registry->save(jsonMeta);
    m_outSource.put("entwine", jsonMeta.toStyledString());

    // Re-allow inserts.
    m_pool->go();
}

Json::Value Builder::saveProps() const
{
    Json::Value props;

    props["bbox"] = m_bbox->toJson();
    props["schema"] = m_schema->toJson();
    props["structure"] = m_structure->toJson();
    props["reprojection"] = m_reprojection->toJson();
    props["manifest"] = m_manifest->toJson();

    props["numPoints"] = static_cast<Json::UInt64>(m_numPoints);
    props["numTossed"] = static_cast<Json::UInt64>(m_numTossed);

    return props;
}

void Builder::loadProps(const Json::Value& props)
{
    m_bbox.reset(new BBox(props["bbox"]));
    m_schema.reset(new Schema(props["schema"]));
    m_structure.reset(new Structure(props["structure"]));
    m_reprojection.reset(new Reprojection(props["reprojection"]));
    m_manifest.reset(new Manifest(props["manifest"]));

    m_numPoints = props["numPoints"].asUInt64();
    m_numTossed = props["numTossed"].asUInt64();
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

    if (!m_outSource.isRemote() && !fs::mkdirp(m_outSource.path()))
    {
        throw std::runtime_error("Couldn't create local build directory");
    }
}

std::string Builder::name() const
{
    std::string name(m_outSource.path());

    // TODO Temporary/hacky.
    const std::size_t pos(name.find_last_of("/\\"));

    if (pos != std::string::npos)
    {
        name = name.substr(pos + 1);
    }

    return name;
}

} // namespace entwine

