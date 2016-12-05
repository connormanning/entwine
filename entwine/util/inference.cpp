/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/inference.hpp>

#include <limits>

#include <entwine/tree/config-parser.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/inference.hpp>
#include <entwine/util/matrix.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const Bounds expander(([]()
    {
        // Use Bounds::set to avoid malformed bounds warning.
        Bounds b;
        b.set(
                Point(
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max()),
                Point(
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest()));
        return b;
    })());

    const Schema xyzSchema(([]()
    {
        DimList dims;
        dims.push_back(DimInfo("X", "floating", 8));
        dims.push_back(DimInfo("Y", "floating", 8));
        dims.push_back(DimInfo("Z", "floating", 8));
        return Schema(dims);
    })());
}

Inference::Inference(
        const std::string path,
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool allowDelta,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        arbiter::Arbiter* arbiter)
    : m_executor()
    , m_path(path)
    , m_tmpPath(tmpPath)
    , m_pointPool(xyzSchema, nullptr)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_allowDelta(allowDelta)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_manifest(m_arbiter->resolve(ConfigParser::directorify(path), verbose))
    , m_index(0)
    , m_dimVec()
    , m_dimSet()
    , m_cesiumify(false)
    , m_transformation()
    , m_mutex()
{ }

Inference::Inference(
        const Manifest& manifest,
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool allowDelta,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        arbiter::Arbiter* arbiter,
        const bool cesiumify)
    : m_executor()
    , m_tmpPath(tmpPath)
    , m_pointPool(xyzSchema, nullptr)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_allowDelta(allowDelta)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_manifest(manifest)
    , m_index(0)
    , m_dimVec()
    , m_dimSet()
    , m_cesiumify(cesiumify)
    , m_transformation()
    , m_mutex()
{ }

void Inference::go()
{
    if (m_pool)
    {
        throw std::runtime_error("Cannot call Inference::go twice");
    }

    bool valid(false);
    m_pool.reset(new Pool(m_threads));
    const std::size_t size(m_manifest.size());

    for (std::size_t i(0); i < size; ++i)
    {
        FileInfo& f(m_manifest.get(i));
        m_index = i;

        if (m_verbose)
        {
            std::cout << i + 1 << " / " << size << ": " << f.path() <<
                std::endl;
        }

        if (m_executor.good(f.path()))
        {
            valid = true;

            if (m_arbiter->isHttpDerived(f.path()))
            {
                const arbiter::http::Headers range(([&f]()
                {
                    arbiter::http::Headers h;
                    h["Range"] = "bytes=0-16384";
                    return h;
                })());

                m_pool->add([this, &f, range]()
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
    }

    m_pool->join();

    if (!valid)
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
    else if (nativeBounds() == expander)
    {
        throw std::runtime_error("No bounds found");
    }

    if (m_cesiumify)
    {
        std::cout << "Transforming inference" << std::endl;
        m_transformation = makeUnique<Transformation>(calcTransformation());
        for (std::size_t i(0); i < m_manifest.size(); ++i)
        {
            auto& f(m_manifest.get(i));
            if (!f.bounds()) throw std::runtime_error("No bounds present");
            f.bounds(m_executor.transform(*f.bounds(), *m_transformation));
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
    const Point p(nativeBounds().mid());
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
            m_executor.transform(nativeBounds(), rotation));
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
    std::unique_ptr<Preview> preview(m_executor.preview(localPath, m_reproj));

    auto update([&fileInfo](std::size_t numPoints, const Bounds& bounds)
    {
        fileInfo.numPoints(numPoints);
        fileInfo.bounds(bounds);
    });

    if (preview)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            fileInfo.srs(preview->srs);

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
                    m_delta = makeUnique<Delta>(scale, Offset(0, 0, 0));
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

        if (m_trustHeaders)
        {
            update(preview->numPoints, preview->bounds);
            return;
        }
    }

    Bounds curBounds(expander);
    std::size_t curNumPoints(0);

    auto tracker([this, &curBounds, &curNumPoints](Cell::PooledStack stack)
    {
        curNumPoints += stack.size();
        for (const auto& cell : stack) curBounds.grow(cell.point());

        // Return the entire stack since we aren't a consumer of this data.
        return stack;
    });

    NormalPooledPointTable table(m_pointPool, tracker, invalidOrigin);

    if (m_executor.run(table, localPath, m_reproj, m_transformation.get()))
    {
        update(curNumPoints, curBounds);
    }
}

void Inference::aggregate()
{
    m_numPoints = makeUnique<std::size_t>(0);
    m_bounds = makeUnique<Bounds>(expander);

    for (std::size_t i(0); i < m_manifest.size(); ++i)
    {
        const auto& f(m_manifest.get(i));

        *m_numPoints += f.numPoints();

        if (const Bounds* current = f.bounds())
        {
            m_bounds->grow(*current);
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
        m_delta->offset() =
            Point::apply([](double d)
            {
                const int64_t v(d);
                if (static_cast<double>(v / 10 * 10) == d) return v;
                else return (v + 10) / 10 * 10;
            },
            m_bounds->mid());

        for (std::size_t i(0); i < m_manifest.size(); ++i)
        {
            auto& f(m_manifest.get(i));

            if (const Bounds* current = f.bounds())
            {
                f.bounds(current->deltify(*m_delta));
            }
        }
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

Bounds Inference::nativeBounds() const
{
    if (!m_bounds) throw std::runtime_error("Inference incomplete");
    return *m_bounds;
}

Schema Inference::schema() const
{
    if (!m_schema) throw std::runtime_error("Inference incomplete");
    else return *m_schema;
}

} // namespace entwine

