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

#include <entwine/types/reprojection.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/util/inference.hpp>

namespace entwine
{

namespace
{
    const BBox expander(([]()
    {
        // Use BBox::set to avoid malformed bounds warning.
        BBox b;
        b.set(
                Point(
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max()),
                Point(
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest()),
                true);
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
    : m_executor(true)
    , m_pools(xyzSchema)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmp(m_arbiter->getEndpoint(tmpPath))
    , m_manifest(m_arbiter->resolve(path, verbose))
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
    : m_executor(true)
    , m_pools(xyzSchema)
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
        if (m_verbose) std::cout << i + 1 << " / " << size << std::endl;

        FileInfo& f(m_manifest.get(i));
        m_index = i;

        if (m_executor.good(f.path()))
        {
            valid = true;

            m_pool->add([&f, &valid, this]()
            {
                auto localHandle(m_arbiter->getLocalHandle(f.path(), m_tmp));
                add(localHandle->localPath(), f);
            });
        }
    }

    m_pool->join();
    if (!valid) throw std::runtime_error("No valid sources to inference.");
    m_done = true;
}

void Inference::add(const std::string localPath, FileInfo& fileInfo)
{
    std::unique_ptr<Preview> preview(m_executor.preview(localPath, m_reproj));

    auto update([&](std::size_t numPoints, const BBox& bbox)
    {
        fileInfo.numPoints(numPoints);
        fileInfo.bbox(bbox);
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
            update(preview->numPoints, preview->bbox);
            return;
        }
    }

    BBox curBBox(expander);
    std::size_t curNumPoints(0);

    auto tracker([this, &curBBox, &curNumPoints](PooledInfoStack infoStack)
    {
        curNumPoints += infoStack.size();
        RawInfoNode* info(infoStack.head());

        while (info)
        {
            curBBox.grow(info->val().point());
            info = info->next();
        }

        // Return the entire stack since we aren't a consumer of this data.
        return infoStack;
    });

    PooledPointTable table(m_pools, tracker);

    if (m_executor.run(table, localPath, m_reproj))
    {
        update(curNumPoints, curBBox);
    }
}

Schema Inference::schema() const
{
    DimList dims;
    for (const auto& name : m_dimVec)
    {
        const pdal::Dimension::Id::Enum id(pdal::Dimension::id(name));
        dims.emplace_back(name, id, pdal::Dimension::defaultType(id));
    }
    return Schema(dims);
}

BBox Inference::bbox() const
{
    BBox bbox(expander);

    for (std::size_t i(0); i < m_manifest.size(); ++i)
    {
        if (const BBox* current = m_manifest.get(i).bbox())
        {
            bbox.grow(*current);
        }
    }

    return bbox;
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

