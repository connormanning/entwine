/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <vector>

#include <entwine/types/sized-point-table.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Schema;

class SimplePointTable : public SizedPointTable
{
public:
    explicit SimplePointTable(DataPool& dataPool, const Schema& schema);

    virtual pdal::PointId addPoint();
    virtual char* getPoint(pdal::PointId index);
    virtual void setField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            const void* value);
    virtual void getField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            void* value);

    void clear();
    std::size_t size() const;

    PooledDataNode* getNode(std::size_t index) const
    {
        return m_dataNodes.at(index);
    }

private:
    char* getDimension(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index);

    DataPool& m_dataPool;
    std::deque<PooledDataNode*> m_dataNodes;
    std::size_t m_numPoints;
};

} // namespace entwine

