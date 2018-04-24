/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/new-reader/new-query.hpp>

#include <entwine/new-reader/new-reader.hpp>

namespace entwine
{

NewQuery::NewQuery(const NewReader& r, const NewQueryParams& p)
    : m_metadata(r.metadata())
    , m_hierarchy(r.hierarchy())
    , m_params(p.finalize(m_metadata))
    , m_filter(m_metadata, m_params)
    , m_table(m_metadata.schema())
    , m_pointRef(m_table, 0)
    , m_overlaps(overlaps())
{
    /*
    Json::Value j;
    for (const auto& p : m_overlaps)
    {
        j[p.first.toString()] = p.second;
    }
    std::cout << j.toStyledString() << std::endl;
    std::cout << "OL: " << m_overlaps.size() << std::endl;
    */
}

HierarchyReader::Keys NewQuery::overlaps() const
{
    HierarchyReader::Keys keys;
    ChunkKey c(m_metadata);
    c.d = m_metadata.structure().body();    // TODO Handle `head`?
    overlaps(keys, c);
    return keys;
}

void NewQuery::overlaps(HierarchyReader::Keys& keys, const ChunkKey& c) const
{
    if (!m_filter.check(c.bounds())) return;

    const auto k(c.get());
    const auto count(m_hierarchy.count(k));
    if (!count) return;

    if (c.depth() > m_params.db()) keys[k] = count;

    if (c.depth() + 1 >= m_params.de()) return;

    if (c.inBody())
    {
        for (std::size_t i(0); i < dirEnd(); ++i)
        {
            overlaps(keys, c.getStep(toDir(i)));
        }
    }
    else
    {
        overlaps(keys, c.getStep());
    }
}

} // namespace entwine

