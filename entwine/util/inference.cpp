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
#include <entwine/types/simple-point-table.hpp>
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
    , m_dataPool(xyzSchema.pointSize(), 4096 * 32)
    , m_reproj(reprojection)
    , m_threads(threads)
    , m_verbose(verbose)
    , m_trustHeaders(trustHeaders)
    , m_valid(false)
    , m_done(false)
    , m_ownedArbiter(arbiter ? nullptr : new arbiter::Arbiter())
    , m_arbiter(arbiter ? arbiter : m_ownedArbiter.get())
    , m_tmpEndpoint(m_arbiter->getEndpoint(tmpPath))
    , m_resolved(m_arbiter->resolve(path, verbose))
    , m_index(0)
    , m_numPoints(0)
    , m_bbox(expander)
    , m_dimVec()
    , m_dimSet()
    , m_mutex()
{
    if (m_verbose)
    {
        std::cout << "Inferring from " << m_resolved.size() << " paths." <<
            std::endl;
    }
}

void Inference::go()
{
    if (m_pool)
    {
        throw std::runtime_error("Cannot call Inference::go twice");
    }

    m_pool.reset(new Pool(m_threads));

    for (const std::string& f : m_resolved)
    {
        ++m_index;
        m_pool->add([f, this]()
        {
            auto localHandle(m_arbiter->getLocalHandle(f, m_tmpEndpoint));
            add(localHandle->localPath(), f);
        });
    }

    m_pool->join();
    m_done = true;
}

void Inference::add(const std::string localPath, const std::string realPath)
{
    std::unique_ptr<Preview> preview(m_executor.preview(localPath, m_reproj));

    auto update([&](std::size_t numPoints, const BBox& bbox)
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        if (m_verbose)
        {
            std::cout << realPath << "\n\t" << bbox << " - " << numPoints <<
                std::endl;
        }

        m_numPoints += numPoints;
        m_bbox.grow(bbox);
        m_bbox.bloat();
    });

    if (preview)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        m_valid = true;

        for (const auto& d : preview->dimNames)
        {
            if (!m_dimSet.count(d))
            {
                m_dimSet.insert(d);
                m_dimVec.push_back(d);
            }
        }
        lock.unlock();

        if (m_trustHeaders)
        {
            update(preview->numPoints, preview->bbox);
            return;
        }
    }

    BBox curBBox(expander);
    std::size_t curNumPoints(0);
    SimplePointTable table(m_dataPool, xyzSchema);

    auto tracker([this, &curBBox, &curNumPoints](pdal::PointView& view)
    {
        Point p;
        curNumPoints += view.size();

        for (std::size_t i = 0; i < view.size(); ++i)
        {
            p.x = view.getFieldAs<double>(pdal::Dimension::Id::X, i);
            p.y = view.getFieldAs<double>(pdal::Dimension::Id::Y, i);
            p.z = view.getFieldAs<double>(pdal::Dimension::Id::Z, i);

            curBBox.grow(p);
        }
    });

    if (m_executor.run(table, localPath, m_reproj, tracker))
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

} // namespace entwine

