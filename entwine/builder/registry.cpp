/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/registry.hpp>

#include <pdal/PointView.hpp>

#include <entwine/io/io.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/unique.hpp>

#include <entwine/io/laszip.hpp>

namespace entwine
{

Registry::Registry(
        const Metadata& metadata,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        ThreadPools& threadPools,
        const bool exists)
    : m_metadata(metadata)
    , m_dataEp(out.getSubEndpoint("ept-data"))
    , m_hierEp(out.getSubEndpoint("ept-hierarchy"))
    , m_tmp(tmp)
    , m_threadPools(threadPools)
    , m_hierarchy(m_metadata, m_hierEp, exists)
    , m_chunkCache(
            makeUnique<ChunkCache>(
                m_hierarchy,
                clipPool(),
                m_dataEp,
                m_tmp,
                m_metadata.cacheSize()))
{ }

void Registry::save(const uint64_t hierarchyStep, const bool verbose)
{
    m_chunkCache.reset();

    if (!m_metadata.subset())
    {
        if (hierarchyStep) m_hierarchy.setStep(hierarchyStep);
        else m_hierarchy.analyze(m_metadata, verbose);
    }

    m_hierarchy.save(m_metadata, m_hierEp, m_threadPools.workPool());
}

void Registry::merge(const Registry& other, Clipper& clipper)
{
    for (const auto& p : other.hierarchy().map())
    {
        const Dxyz& dxyz(p.first);
        const uint64_t np(p.second);

        if (dxyz.d < m_metadata.sharedDepth())
        {
            VectorPointTable table(m_metadata.schema(), np);
            table.setProcess([this, &table, &clipper, &dxyz]()
            {
                Voxel voxel;
                Key pk(m_metadata);
                ChunkKey ck(m_metadata);

                for (auto it(table.begin()); it != table.end(); ++it)
                {
                    voxel.initShallow(it.pointRef(), it.data());
                    const Point point(voxel.point());
                    pk.init(point, dxyz.d);
                    ck.init(point, dxyz.d);

                    m_chunkCache->insert(voxel, pk, ck, clipper);
                }
            });

            const auto filename(
                    dxyz.toString() + other.metadata().postfix(dxyz.d));
            m_metadata.dataIo().read(m_dataEp, m_tmp, filename, table);
        }
        else
        {
            assert(!m_hierarchy.get(dxyz));
            m_hierarchy.set(dxyz, np);
        }
    }
}

} // namespace entwine

