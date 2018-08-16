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

Query::Query(const Reader& r, const Json::Value& j)
    : m_reader(r)
    , m_metadata(r.metadata())
    , m_hierarchy(r.hierarchy())
    , m_params(j)
    , m_filter(m_metadata, m_params)
    , m_table(m_metadata.schema())
    , m_pointRef(m_table, 0)
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
            for (const auto& cell : chunk->cells())
            {
                maybeProcess(cell);
            }
        }
    }
}

void Query::maybeProcess(const Cell& cell)
{
    if (!m_params.bounds().contains(cell.point())) return;
    m_table.setPoint(cell.uniqueData());
    if (!m_filter.check(m_pointRef)) return;
    process(cell);
    ++m_numPoints;
}

void ReadQuery::process(const Cell& cell)
{
    m_data.resize(m_data.size() + m_schema.pointSize(), 0);
    char* pos(m_data.data() + m_data.size() - m_schema.pointSize());

    std::size_t dimNum(0);
    double d(0);

    for (const auto& dimInfo : m_schema.dims())
    {
        dimNum = pdal::Utils::toNative(dimInfo.id()) - 1;

        if (dimNum < 3 && m_delta.exists())
        {
            d = Point::scale(
                    m_pointRef.getFieldAs<double>(dimInfo.id()),
                    m_delta.scale()[dimNum],
                    m_delta.offset()[dimNum]);

            setAs(pos, d, dimInfo.type());
        }
        else
        {
            m_pointRef.getField(pos, dimInfo.id(), dimInfo.type());
        }

        pos += dimInfo.size();
    }
}

} // namespace entwine

