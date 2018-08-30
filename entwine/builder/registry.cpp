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

namespace entwine
{

Registry::Registry(
        const Metadata& metadata,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        ThreadPools& threadPools,
        const bool exists)
    : m_metadata(metadata)
    , m_out(out)
    , m_tmp(tmp)
    , m_pointPool(pointPool)
    , m_threadPools(threadPools)
    , m_hierarchy(m_metadata, out, exists)
    , m_root(ChunkKey(metadata), out, tmp, pointPool, m_hierarchy)
{ }

void Registry::save(const arbiter::Endpoint& endpoint) const
{
    m_hierarchy.save(m_metadata, endpoint, m_threadPools.workPool());
}

void Registry::merge(const Registry& other, Clipper& clipper)
{
    /*
    for (const auto& p : other.hierarchy().map())
    {
        const Dxyz& dxyz(p.first);
        const uint64_t np(p.second);

        if (dxyz.d < m_metadata.sharedDepth())
        {
            auto cells(m_metadata.dataIo().read(
                        m_out,
                        m_tmp,
                        m_pointPool,
                        dxyz.toString() + other.metadata().postfix(dxyz.d)));

            Key pk(m_metadata);

            while (!cells.empty())
            {
                auto cell(cells.popOne());
                pk.init(cell->point(), dxyz.d);

                ReffedChunk* rc(&m_root);
                for (std::size_t d(0); d < dxyz.d; ++d)
                {
                    rc = &rc->chunk().step(cell->point());
                }

                if (!rc->insert(cell, pk, clipper))
                {
                    std::cout << "Invalid merge insert: " <<  dxyz.toString() <<
                        std::endl;
                }
            }
        }
        else
        {
            assert(!m_hierarchy.get(dxyz));
            m_hierarchy.set(dxyz, np);
        }
    }
    */
}

} // namespace entwine

