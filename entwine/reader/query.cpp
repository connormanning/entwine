/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/query.hpp>

#include <entwine/reader/reader.hpp>

namespace entwine
{

Query::Query(const Reader& r, const json& j)
    : m_reader(r)
    , m_metadata(r.metadata())
    , m_hierarchy(r.hierarchy())
    , m_params(j)
    , m_filter(m_metadata, m_params)
    , m_overlaps(overlaps())
{ }

HierarchyReader::Keys Query::overlaps() const
{
    HierarchyReader::Keys keys;
    ChunkKey c(m_metadata);
    overlaps(keys, c);
    return keys;
}

void Query::overlaps(HierarchyReader::Keys& keys, const ChunkKey& c) const
{
    if (!m_filter.check(c.bounds())) return;

    const auto k(c.get());
    const auto count(m_hierarchy.count(k));
    if (!count) return;

    if (c.depth() >= m_params.db()) keys[k] = count;

    if (c.depth() + 1 >= m_params.de()) return;

    for (std::size_t i(0); i < dirEnd(); ++i)
    {
        overlaps(keys, c.getStep(toDir(i)));
    }
}

void Query::run()
{
    for (const auto& k : m_overlaps)
    {
        // For now we're doing one at a time.
        std::vector<Dxyz> keys;
        keys.push_back(k.first);
        auto block(m_reader.cache().acquire(m_reader, keys));

        for (auto& chunk : block)
        {
            for (const auto& pr : chunk->table())
            {
                maybeProcess(pr);
            }
        }
    }
}

void Query::maybeProcess(const pdal::PointRef& pr)
{
    const Point point(
            pr.getFieldAs<double>(DimId::X),
            pr.getFieldAs<double>(DimId::Y),
            pr.getFieldAs<double>(DimId::Z));
    if (!m_params.bounds().contains(point) || !m_filter.check(pr)) return;
    process(pr);
    ++m_points;
}

void ReadQuery::process(const pdal::PointRef& pr)
{
    m_data.resize(m_data.size() + m_schema.pointSize(), 0);
    char* pos(m_data.data() + m_data.size() - m_schema.pointSize());

    for (const auto& dimInfo : m_schema.dims())
    {
        pr.getField(pos, dimInfo.id(), dimInfo.type());
        pos += dimInfo.size();
    }
}

} // namespace entwine

