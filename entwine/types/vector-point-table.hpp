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

#include <cstddef>
#include <stdexcept>
#include <vector>

#include <pdal/PointRef.hpp>
#include <pdal/PointTable.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

class VectorPointTable : public pdal::StreamPointTable
{
public:
    // Constructible with either an entwine::Schema or a pdal::PointLayout.
    // The constructors which take a Schema defer to their PointLayout versions.
    VectorPointTable(const Schema& schema)
        : VectorPointTable(schema.pdalLayout())
    { }

    VectorPointTable(const Schema& schema, std::size_t initialNumPoints)
        : VectorPointTable(schema.pdalLayout(), initialNumPoints)
    { }

    VectorPointTable(const Schema& schema, const std::vector<char>& data)
        : VectorPointTable(schema.pdalLayout(), data)
    { }

    VectorPointTable(const Schema& schema, std::vector<char>&& data)
        : VectorPointTable(schema.pdalLayout(), std::move(data))
    { }

    VectorPointTable(pdal::PointLayout layout)
        : StreamPointTable(m_layout)
        , m_layout(layout)
    { }

    VectorPointTable(pdal::PointLayout layout, std::size_t initialNumPoints)
        : StreamPointTable(m_layout)
        , m_layout(layout)
    {
        resize(initialNumPoints);
    }

    VectorPointTable(pdal::PointLayout layout, const std::vector<char>& data)
        : pdal::StreamPointTable(m_layout)
        , m_layout(layout)
        , m_data(data)
        , m_size(m_data.size() / m_layout.pointSize())
    { }

    VectorPointTable(pdal::PointLayout layout, std::vector<char>&& data)
        : pdal::StreamPointTable(m_layout)
        , m_layout(layout)
        , m_data(std::move(data))
        , m_size(m_data.size() / m_layout.pointSize())
    { }

    std::size_t size() const { return m_size; }
    pdal::point_count_t capacity() const override { return size(); }

    pdal::PointRef at(pdal::PointId index)
    {
        if (index >= size())
        {
            throw std::out_of_range("Invalid index to VectorPointTable::at");
        }

        return pdal::PointRef(*this, index);
    }

    void assign(std::vector<char>&& data)
    {
        m_data = std::move(data);
        m_size = m_data.size() / m_layout.pointSize();
    }

    pdal::PointRef append()
    {
        return pdal::PointRef(*this, addPoint());
    }

    virtual char* getPoint(pdal::PointId index) override
    {
        return m_data.data() + pointsToBytes(index);
    }

    std::vector<char>& data() { return m_data; }
    const std::vector<char>& data() const { return m_data; }

    std::vector<char>&& acquire() { return std::move(m_data); }

    void resize(std::size_t numPoints)
    {
        m_data.resize(pointsToBytes(numPoints), 0);
        m_size = numPoints;
    }

    class Iterator
    {
    public:
        explicit Iterator(VectorPointTable& table, pdal::PointId index = 0)
            : m_table(table)
            , m_index(index)
            , m_pointRef(m_table, m_index)
        { }

        Iterator& operator++()
        {
            m_pointRef.setPointId(++m_index);
            return *this;
        }

        Iterator operator++(int)
        {
            Iterator other(*this);
            ++other;
            return other;
        }

        pdal::PointRef& operator*() { return m_pointRef; }
        bool operator!=(const Iterator& rhs) { return m_index != rhs.m_index; }
        char* data() { return m_table.getPoint(m_index); }

    private:
        VectorPointTable& m_table;
        pdal::PointId m_index;
        pdal::PointRef m_pointRef;
    };

    Iterator begin() { return Iterator(*this); }
    Iterator end() { return Iterator(*this, capacity()); }

    std::size_t pointSize() const { return m_layout.pointSize(); }

private:
    virtual pdal::PointId addPoint() override
    {
        m_data.insert(m_data.end(), m_layout.pointSize(), 0);
        return m_size++;
    }

    VectorPointTable(const VectorPointTable&);
    VectorPointTable& operator=(const VectorPointTable&);

    pdal::PointLayout m_layout;
    std::vector<char> m_data;
    std::size_t m_size = 0;
};

} // namespace entwine

