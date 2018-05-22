/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/registry.hpp>

#include <pdal/PointView.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Registry::Registry(
        const Metadata& metadata,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        Pool& threadPool,
        const bool exists)
    : m_metadata(metadata)
    , m_out(out)
    , m_tmp(tmp)
    , m_pointPool(pointPool)
    , m_threadPool(threadPool)
    , m_hierarchy(exists ?
            parse(m_out.get(
                    "entwine-hierarchy" + m_metadata.postfix() + ".json")) :
            Json::Value())
    , m_root(ChunkKey(metadata), out, tmp, pointPool, m_hierarchy)
{ }

void Registry::save(const arbiter::Endpoint& endpoint) const
{
    const std::string f("entwine-hierarchy" + m_metadata.postfix() + ".json");
    io::ensurePut(endpoint, f, m_hierarchy.toJson().toStyledString());
}

void Registry::merge(const Registry& other, Clipper& clipper)
{
    const auto& s(m_metadata.structure());

    for (const auto& p : other.hierarchy().map())
    {
        const Dxyz& dxyz(p.first);
        const uint64_t np(p.second);

        if (dxyz.d < s.shared())
        {
            auto cells(m_metadata.storage().read(
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
                for (std::size_t d(s.body()); d < dxyz.d; ++d)
                {
                    rc = &rc->chunk().step(cell->point());
                }

                if (!rc->insert(cell, pk, clipper))
                {
                    throw std::runtime_error(
                            "Invalid merge insert: " + dxyz.toString());
                }
            }
        }
        else
        {
            assert(!m_hierarchy.get(dxyz));
            m_hierarchy.set(dxyz, np);
        }
    }
}

} // namespace entwine

