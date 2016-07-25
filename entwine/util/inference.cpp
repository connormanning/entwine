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

#include <pdal/PointView.hpp>

#include <entwine/tree/config-parser.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/inference.hpp>

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
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        const Reprojection* reprojection,
        const bool trustHeaders,
        arbiter::Arbiter* arbiter)
    : m_executor()
    , m_pointPool(xyzSchema)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_manifest(m_arbiter->resolve(ConfigParser::directorify(path), verbose))
    , m_index(0)
    , m_dimVec()
    , m_dimSet()
    , m_mutex()
{ }

Inference::Inference(
        const Manifest& manifest,
        const std::string tmpPath,
        const std::size_t threads,
        const bool verbose,
        const Reprojection* reprojection,
        const bool trustHeaders,
        arbiter::Arbiter* arbiter)
    : m_executor()
    , m_pointPool(xyzSchema)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_manifest(manifest)
    , m_index(0)
    , m_dimVec()
    , m_dimSet()
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
    else if (!numPoints())
    {
        throw std::runtime_error("Zero points found");
    }
    else if (!schema().pointSize())
    {
        throw std::runtime_error("No schema dimensions found");
    }
    else if (bounds() == expander)
    {
        throw std::runtime_error("No bounds found");
    }

    m_done = true;
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

    PooledPointTable table(m_pointPool, tracker);

    if (m_executor.run(table, localPath, m_reproj))
    {
        update(curNumPoints, curBounds);
    }
}

Schema Inference::schema() const
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
    return Schema(dims);
}

Bounds Inference::bounds() const
{
    Bounds bounds(expander);

    for (std::size_t i(0); i < m_manifest.size(); ++i)
    {
        if (const Bounds* current = m_manifest.get(i).bounds())
        {
            bounds.grow(*current);
        }
    }

    return bounds;
}

std::size_t Inference::numPoints() const
{
    std::size_t numPoints(0);
    for (std::size_t i(0); i < m_manifest.size(); ++i)
    {
        numPoints += m_manifest.get(i).numPoints();
    }

    return numPoints;
}

} // namespace entwine

