/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/inference.hpp>

#include <limits>

#include <entwine/tree/builder.hpp>
#include <entwine/tree/config-parser.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/matrix.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const arbiter::http::Headers range(([]()
    {
        arbiter::http::Headers h;
        h["Range"] = "bytes=0-16384";
        return h;
    })());

    const Schema xyzSchema({
        { pdal::Dimension::Id::X },
        { pdal::Dimension::Id::Y },
        { pdal::Dimension::Id::Z }
    });
}

Inference::Inference(
        const FileInfoList& fileInfo,
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool allowDelta,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        const bool cesiumify,
        arbiter::Arbiter* arbiter)
    : m_tmpPath(tmpPath)
    , m_pointPool(xyzSchema, nullptr)
    , m_reproj(maybeClone(reprojection))
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_allowDelta(allowDelta)
    , m_cesiumify(cesiumify)
    , m_ownedArbiter(arbiter ? nullptr : makeUnique<arbiter::Arbiter>())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_fileInfo(fileInfo)
{
    if (m_allowDelta) m_delta = makeUnique<Delta>(0.01, Offset(0));
}

Inference::Inference(Builder& builder, const FileInfoList& fileInfo)
    : Inference(
            fileInfo,
            builder.metadata().reprojection(),
            builder.metadata().trustHeaders(),
            false,  // Delta won't matter - we will just use the file-info.
            builder.tmpEndpoint().prefixedRoot(),
            builder.threadPools().size(),
            builder.verbose(),
            false,  // Adding to existing index isn't allows for Cesium.
            &builder.arbiter())
{ }

Inference::Inference(
        const Paths& paths,
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool allowDelta,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        const bool cesiumify,
        arbiter::Arbiter* arbiter)
    : Inference(
            FileInfoList(),
            reprojection,
            trustHeaders,
            allowDelta,
            tmpPath,
            threads,
            verbose,
            cesiumify,
            arbiter)
{
    for (const auto& p : paths)
    {
        const std::string expanded(ConfigParser::directorify(p));
        auto resolved(m_arbiter->resolve(expanded, m_verbose));
        std::sort(resolved.begin(), resolved.end());
        for (const auto& f : resolved)
        {
            m_fileInfo.emplace_back(f);
        }
    }
}

Inference::Inference(
        const std::string path,
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool allowDelta,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        const bool cesiumify,
        arbiter::Arbiter* arbiter)
    : Inference(
            FileInfoList(),
            reprojection,
            trustHeaders,
            allowDelta,
            tmpPath,
            threads,
            verbose,
            cesiumify,
            arbiter)
{
    const std::string expanded(ConfigParser::directorify(path));
    auto resolved(m_arbiter->resolve(expanded, m_verbose));
    std::sort(resolved.begin(), resolved.end());
    for (const auto& f : resolved)
    {
        m_fileInfo.emplace_back(f);
    }
}

void Inference::go()
{
    if (m_pool || m_valid)
    {
        throw std::runtime_error("Cannot call Inference::go twice");
    }

    m_pool = makeUnique<Pool>(m_threads);
    const std::size_t size(m_fileInfo.size());

    for (std::size_t i(0); i < size; ++i)
    {
        FileInfo& f(m_fileInfo[i]);
        m_index = i;

        if (m_verbose)
        {
            std::cout << i + 1 << " / " << size << ": " << f.path() <<
                std::endl;
        }

        if (Executor::get().good(f.path()))
        {
            m_valid = true;

            if (m_arbiter->isHttpDerived(f.path()))
            {
                m_pool->add([this, &f]()
                {
                    const auto data(m_arbiter->getBinary(f.path(), range));

                    std::string name(f.path());
                    std::replace(name.begin(), name.end(), '/', '-');
                    std::replace(name.begin(), name.end(), '\\', '-');

                    m_tmp.put(name, data);

                    add(m_tmp.fullPath(name), f);

                    arbiter::fs::remove(m_tmp.fullPath(name));
                });
            }
            else
            {
                m_pool->add([&f, this]()
                {
                    auto localHandle(
                        m_arbiter->getLocalHandle(f.path(), m_tmp));

                    add(localHandle->localPath(), f);
                });
            }
        }
        else
        {
            f.status(FileInfo::Status::Omitted);
        }
    }

    m_pool->join();

    if (!m_valid)
    {
        throw std::runtime_error("No point cloud files found");
    }

    aggregate();
    makeSchema();

    if (!numPoints())
    {
        throw std::runtime_error("Zero points found");
    }
    else if (!schema().pointSize())
    {
        throw std::runtime_error("No schema dimensions found");
    }
    else if (bounds() == Bounds::expander())
    {
        throw std::runtime_error("No bounds found");
    }

    if (m_cesiumify)
    {
        std::cout << "Transforming inference" << std::endl;
        m_transformation = makeUnique<Transformation>(calcTransformation());

        m_bounds = makeUnique<Bounds>(Bounds::expander());
        for (auto& f : m_fileInfo)
        {
            if (f.bounds() && f.numPoints())
            {
                f.bounds(
                        Executor::get().transform(
                            *f.bounds(),
                            *m_transformation));

                m_bounds->grow(*f.bounds());
            }
        }
    }

    m_done = true;
}

Transformation Inference::calcTransformation()
{
    // We have our full bounds and info for all files in EPSG:4978.  Now:
    //      1) determine a transformation matrix so outward is up
    //      2) transform our file info and bounds accordingly

    // We're going to use our Point class to represent Vectors in this function.
    using Vector = Point;

    // Let O = (0,0,0) be the origin (center of the earth).  This is our native
    // projection system with unit vectors i=(1,0,0), j=(0,1,0), and k=(0,0,1).

    // Let P = bounds.mid(), our transformed origin point.

    // Let S be the sphere centered at O with radius ||P||.

    // Let T = the plane tangent to S at P.

    // Now, we can define our desired coordinate system:
    //
    // k' = "up" = normalized vector O->P
    //
    // j' = "north" = the normalized projected vector onto tangent plane T, of
    // the north pole vector (0,0,1) from the non-transformed coordinate system.
    //
    // i' = "east" = j' cross k'

    // Determine normalized vector k'.
    const Point p(bounds().mid());
    const Vector up(Vector::normalize(p));

    // Project the north pole vector onto k'.
    const Vector northPole(0, 0, 1);
    const double dot(Point::dot(up, northPole));
    const Vector proj(up * dot);

    // Subtract that projection from the north pole vector to project it onto
    // tangent plane T - then normalize to determine vector j'.
    const Vector north(Vector::normalize(northPole - proj));

    // Finally, calculate j' cross k' to determine i', which should turn out to
    // be normalized since the inputs are orthogonal and normalized.
    const Vector east(Vector::cross(north, up));

    // First, rotate so up is outward from the center of the earth.
    const std::vector<double> rotation
    {
        east.x,     east.y,     east.z,     0,
        north.x,    north.y,    north.z,    0,
        up.x,       up.y,       up.z,       0,
        0,          0,          0,          1
    };

    // Then, translate around our current best guess at a center point.  This
    // should be close enough to the origin for reasonable precision.
    const Bounds tentativeCenter(
            Executor::get().transform(bounds(), rotation));
    const std::vector<double> translation
    {
        1, 0, 0, -tentativeCenter.mid().x,
        0, 1, 0, -tentativeCenter.mid().y,
        0, 0, 1, -tentativeCenter.mid().z,
        0, 0, 0, 1
    };

    return matrix::multiply(translation, rotation);
}

void Inference::add(const std::string localPath, FileInfo& fileInfo)
{
    std::unique_ptr<Preview> preview(
            Executor::get().preview(localPath, m_reproj.get()));

    auto update([&fileInfo](
                std::size_t numPoints,
                const Bounds& bounds,
                const Json::Value* metadata)
    {
        fileInfo.numPoints(numPoints);
        fileInfo.bounds(bounds);
        if (metadata) fileInfo.metadata(*metadata);
    });

    if (preview)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            fileInfo.srs(preview->srs);

            if (preview->numPoints)
            {
                if (preview->scale)
                {
                    const auto& scale(*preview->scale);

                    if (!scale.x || !scale.y || !scale.z)
                    {
                        throw std::runtime_error(
                                "Invalid scale at " + fileInfo.path());
                    }

                    if (m_delta)
                    {
                        m_delta->scale() = Point::min(m_delta->scale(), scale);
                    }
                    else if (m_allowDelta)
                    {
                        m_delta = makeUnique<Delta>(scale, Offset(0));
                    }
                }

                for (const auto& d : preview->dimNames)
                {
                    if (!m_dimSet.count(d))
                    {
                        m_dimSet.insert(d);
                        m_dimVec.push_back(d);
                    }
                }
            }
        }

        if (m_trustHeaders)
        {
            update(preview->numPoints, preview->bounds, &preview->metadata);
            return;
        }
    }

    Bounds curBounds(Bounds::expander());
    std::size_t curNumPoints(0);

    auto tracker([this, &curBounds, &curNumPoints](Cell::PooledStack stack)
    {
        curNumPoints += stack.size();
        for (const auto& cell : stack) curBounds.grow(cell.point());

        // Return the entire stack since we aren't a consumer of this data.
        return stack;
    });

    PooledPointTable table(m_pointPool, tracker, invalidOrigin);

    if (Executor::get().run(
                table,
                localPath,
                m_reproj.get(),
                m_transformation.get()))
    {
        if (!curNumPoints) curBounds = Bounds();

        update(curNumPoints, curBounds, nullptr);

        if (curNumPoints)
        {
            for (const auto& d : Executor::get().dims(localPath))
            {
                if (!m_dimSet.count(d))
                {
                    m_dimSet.insert(d);
                    m_dimVec.push_back(d);
                }
            }
        }
    }
}

void Inference::aggregate()
{
    m_numPoints = makeUnique<std::size_t>(0);
    m_bounds = makeUnique<Bounds>(Bounds::expander());

    for (const auto& f : m_fileInfo)
    {
        *m_numPoints += f.numPoints();

        if (f.numPoints())
        {
            if (const Bounds* current = f.bounds())
            {
                m_bounds->grow(*current);
            }
        }

        if (!f.srs().empty())
        {
            const auto& s(f.srs().getWKT());
            if (!std::count(m_srsList.begin(), m_srsList.end(), s))
            {
                m_srsList.push_back(s);
            }
        }
    }

    if (m_delta)
    {
        // Since the delta bounds guarantee us an extra buffer of at least 20,
        // we can slop this by 10 for prettier numbers.
        m_delta->offset() = m_bounds->mid().apply([](double d)
        {
            const int64_t v(d);
            if (static_cast<double>(v / 10 * 10) == d) return v;
            else return (v + 10) / 10 * 10;
        });

        m_delta->scale() = m_delta->scale().apply([](double d)->double
        {
            const double epsilon(1e-6);
            double mult(10);
            while (std::round(d * mult) < 1.0) mult *= 10;

            if (d * mult - 1.0 < epsilon)
            {
                switch (static_cast<int>(mult))
                {
                    case int(1e1): return 1e-1;
                    case int(1e2): return 1e-2;
                    case int(1e3): return 1e-3;
                    case int(1e4): return 1e-4;
                    case int(1e5): return 1e-5;
                    case int(1e6): return 1e-6;
                    case int(1e7): return 1e-7;
                    case int(1e8): return 1e-8;
                    case int(1e9): return 1e-9;
                    default: break;
                }
            }
            return d;
        });
    }
}

void Inference::makeSchema()
{
    DimList dims;

    for (const auto& name : m_dimVec)
    {
        const pdal::Dimension::Id id(pdal::Dimension::id(name));

        pdal::Dimension::Type t;
        try
        {
            t = pdal::Dimension::defaultType(id);
        }
        catch (pdal::pdal_error&)
        {
            t = pdal::Dimension::Type::Double;
        }

        dims.emplace_back(name, id, t);
    }

    m_schema = makeUnique<Schema>(dims);

    if (const Delta* d = delta())
    {
        const Bounds cube(m_bounds->cubeify(*d));
        m_schema = makeUnique<Schema>(Schema::deltify(cube, *d, *m_schema));
    }
}

std::size_t Inference::numPoints() const
{
    if (!m_numPoints) throw std::runtime_error("Inference incomplete");
    else return *m_numPoints;
}

Bounds Inference::bounds() const
{
    if (!m_bounds) throw std::runtime_error("Inference incomplete");
    return *m_bounds;
}

Schema Inference::schema() const
{
    if (!m_schema) throw std::runtime_error("Inference incomplete");
    else return *m_schema;
}

Json::Value Inference::toJson() const
{
    Json::Value json;
    json["fileInfo"] = toJsonArrayOfObjects(m_fileInfo);
    json["schema"] = schema().toJson();
    json["bounds"] = bounds().toJson();
    json["numPoints"] = Json::UInt64(numPoints());
    if (m_reproj) json["reprojection"] = m_reproj->toJson();
    if (m_delta) m_delta->insertInto(json);
    if (m_transformation)
    {
        json["transformation"] = toJsonArray(*m_transformation);
    }

    return json;
}

Inference::Inference(const Json::Value& json)
    : m_pointPool(xyzSchema, nullptr)
    , m_valid(true)
    , m_transformation(json.isMember("transformation") ?
            makeUnique<Transformation>(
                extract<double>(json["transformation"])) :
            nullptr)
    , m_ownedArbiter(makeUnique<arbiter::Arbiter>())
    , m_arbiter(m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint("tmp"))
    , m_numPoints(makeUnique<std::size_t>(json["numPoints"].asUInt64()))
    , m_bounds(makeUnique<Bounds>(json["bounds"]))
    , m_schema(makeUnique<Schema>(json["schema"]))
    , m_delta(Delta::maybeCreate(json))
    , m_fileInfo(toFileInfo(json["fileInfo"]))
{ }

} // namespace entwine

