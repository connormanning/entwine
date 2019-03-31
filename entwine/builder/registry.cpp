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

#include <entwine/builder/chunk.hpp>
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
    , m_root(ChunkKey(metadata), m_dataEp, tmp, m_hierarchy)
    , m_chunkCache(
            makeUnique<ChunkCache>(
                m_hierarchy,
                clipPool(),
                m_dataEp,
                m_tmp))
{ }

void Registry::save()
{
    // TODO This won't be necessary once the clipping logic is hooked up.
    m_chunkCache.reset();

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

                for (auto it(table.begin()); it != table.end(); ++it)
                {
                    voxel.initShallow(it.pointRef(), it.data());
                    const Point point(voxel.point());
                    pk.init(point, dxyz.d);

                    ReffedChunk* rc(&m_root);
                    for (uint64_t d(0); d < dxyz.d; ++d)
                    {
                        rc = &rc->chunk().step(point);
                    }

                    rc->insert(voxel, pk, clipper);
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

