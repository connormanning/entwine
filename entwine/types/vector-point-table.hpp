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

#include <entwine/types/point-pool.hpp>

namespace entwine
{

// For writing.
class ShallowPointTable : public pdal::SimplePointTable
{
public:
    ShallowPointTable(const Schema& schema, Data::RawStack& stack)
        : SimplePointTable(schema.pdalLayout())
        , m_stack(stack)
    {
        m_refs.reserve(m_stack.size());
        for (char* p : stack) m_refs.push_back(p);
    }

    virtual char* getPoint(pdal::PointId index) override
    {
        return m_refs[index];
    }

    virtual pdal::PointId addPoint() override
    {
        // throw std::runtime_error("Cannot add points to ShallowPointTable");
        return m_index++;
    }

    std::size_t size() const { return m_stack.size(); }

private:
    Data::RawStack& m_stack;
    std::vector<char*> m_refs;
    std::size_t m_index = 0;
};

// For reading.
class VectorPointTable : public pdal::StreamPointTable
{
    using Process = std::function<void()>;

public:
    VectorPointTable(const Schema& schema, std::size_t np)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pointSize(schema.pointSize())
    {
        m_data.resize(this->pointsToBytes(np), 0);
    }

    /*
    VectorPointTable(const Schema& schema,  const std::vector<char>& data)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pointSize(schema.pointSize())
        , m_data(data)
        , m_size(m_data.size() / m_pointSize)
    { }

    VectorPointTable(const Schema& schema, std::vector<char>&& data)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pointSize(schema.pointSize())
        , m_data(std::move(data))
        , m_size(m_data.size() / m_pointSize)
    { }
    */

    std::size_t size() const { return m_size; }
    pdal::point_count_t capacity() const override { return m_data.size() /
        m_pointSize; }

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
        m_size = m_data.size() / m_pointSize;
    }

    pdal::PointRef append()
    {
        return pdal::PointRef(*this, addPoint());
    }

    virtual char* getPoint(pdal::PointId index) override
    {
        return m_data.data() + this->pointsToBytes(index);
    }

    std::vector<char>& data() { return m_data; }
    const std::vector<char>& data() const { return m_data; }

    std::vector<char>&& acquire() { return std::move(m_data); }

    void setProcess(Process f) { m_f =f; }
    void reset() override { m_f(); }

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
        pdal::PointRef& pointRef() { return m_pointRef; }
        bool operator!=(const Iterator& rhs) { return m_index != rhs.m_index; }
        char* data() { return m_table.getPoint(m_index); }

    private:
        VectorPointTable& m_table;
        pdal::PointId m_index;
        pdal::PointRef m_pointRef;
    };

    Iterator begin() { return Iterator(*this); }
    Iterator end() { return Iterator(*this, size()); }

    std::size_t pointSize() const { return m_pointSize; }

private:
    /*
    virtual pdal::PointId addPoint() override
    {
        // m_data.insert(m_data.end(), m_pointSize, 0);
        return m_size++;
    }
    */

    virtual void setSize(pdal::PointId s) override
    {
        m_size = s;
    }

    VectorPointTable(const VectorPointTable&);
    VectorPointTable& operator=(const VectorPointTable&);

    const std::size_t m_pointSize;
    std::vector<char> m_data;
    std::size_t m_size = 0;

    Process m_f = []() { };
};

} // namespace entwine

