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
    : m_reader(r)
    , m_metadata(r.metadata())
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

    if (c.depth() >= m_params.db()) keys[k] = count;

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

void NewQuery::run()
{
    // Fetch all touched chunks (for now).
    for (const auto& key : m_overlaps)
    {
        NewChunkReader chunk(m_reader, key.first);
        for (const auto& cell : chunk.cells())
        {
            maybeProcess(cell);
        }
    }
}

void NewQuery::maybeProcess(const Cell& cell)
{
    if (!m_params.bounds().contains(cell.point())) return;
    m_table.setPoint(cell.uniqueData());
    if (!m_filter.check(m_pointRef)) return;
    process(cell);
    ++m_numPoints;
}

void NewReadQuery::process(const Cell& cell)
{
    m_data.resize(m_data.size() + m_schema.pointSize(), 0);
    char* pos(m_data.data() + m_data.size() - m_schema.pointSize());

    std::size_t dimNum(0);

    for (const auto& dimInfo : m_schema.dims())
    {
        // const DimInfo& dimInfo(dim.info());
        dimNum = pdal::Utils::toNative(dimInfo.id()) - 1;

        if (dimNum < 3 &&
                (m_params.delta().exists() || m_params.nativeBounds()))
        {
            setScaled(dimInfo, dimNum, pos);
        }
        else
        {
            m_pointRef.getField(pos, dimInfo.id(), dimInfo.type());
        }

        pos += dimInfo.size();
    }
}

} // namespace entwine

