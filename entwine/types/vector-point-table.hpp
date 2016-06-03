/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cstddef>
#include <vector>

#include <pdal/PointTable.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

class VectorPointTable : public pdal::BasePointTable
{
public:
    VectorPointTable(const Schema& schema, std::vector<char>& data)
        : BasePointTable(schema.pdalLayout())
        , m_schema(schema)
        , m_data(data)
    { }

    std::size_t size() const { return m_data.size() / m_schema.pointSize(); }

    virtual char* getPoint(pdal::PointId i)
    {
        return m_data.data() + (i * m_schema.pointSize());
    }

private:
    virtual void setFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            const void* pos)
    {
        throw std::runtime_error("VectorPointTable is read-only");
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id::Enum dimId,
            pdal::PointId i,
            void* pos) const
    {
        const pdal::Dimension::Detail& dimDetail(
                *m_schema.pdalLayout().dimDetail(dimId));
        const char* src(getPoint(i) + dimDetail.offset());

        std::copy(src, src + dimDetail.size(), static_cast<char*>(pos));
    }

    virtual pdal::PointId addPoint()
    {
        throw std::runtime_error("Cannot add points to a TiledPointTable");
    }

    const char* getPoint(pdal::PointId i) const
    {
        return m_data.data() + (i * m_schema.pointSize());
    }

    const Schema& m_schema;
    std::vector<char>& m_data;
};

} // namespace entwine

