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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/util/inference.hpp>
#include <entwine/util/pool.hpp>

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
        const Reprojection* reprojection,
        const bool trustHeaders,
        const bool cubeify,
        arbiter::Arbiter* arbiter)
    : m_executor(true)
    , m_dataPool(xyzSchema.pointSize(), 4096 * 32)
    , m_reproj(reprojection)
    , m_trustHeaders(trustHeaders)
    , m_cubeify(cubeify)
    , m_numPoints(0)
    , m_bbox(expander)
    , m_dimVec()
    , m_dimSet()
    , m_mutex()
{
    std::unique_ptr<arbiter::Arbiter> ownedArbiter;

    if (!arbiter)
    {
        ownedArbiter.reset(new arbiter::Arbiter());
        arbiter = ownedArbiter.get();
    }

    auto tmpEndpoint(arbiter->getEndpoint(tmpPath));
    auto resolved(arbiter->resolve(path));

    // TODO Take in thread count.
    entwine::Pool pool(8);

    for (const std::string& f : resolved)
    {
        pool.add([arbiter, f, &tmpEndpoint, this]()
        {
            auto localHandle(arbiter->getLocalHandle(f, tmpEndpoint));
            add(localHandle->localPath());
        });
    }

    pool.join();
}

void Inference::add(const std::string localPath)
{
    std::unique_ptr<Preview> preview(m_executor.preview(localPath, m_reproj));

    if (preview)
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

        if (m_trustHeaders)
        {
            m_numPoints += preview->numPoints;
            m_bbox.grow(preview->bbox);
            if (m_cubeify) m_bbox.cubeify();
            m_bbox.bloat();

            return;
        }
    }

    BBox curBBox(expander);
    std::size_t curNumPoints(0);
    SimplePointTable table(m_dataPool, xyzSchema);

    auto bounder([this, &curBBox, &curNumPoints](pdal::PointView& view)->void
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

    if (!m_executor.run(table, localPath, m_reproj, bounder))
    {
        m_numPoints += curNumPoints;
        m_bbox.grow(curBBox);
        if (m_cubeify) m_bbox.cubeify();
        m_bbox.bloat();
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

