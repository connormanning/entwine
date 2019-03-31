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

class MemBlock
{
public:
    using Block = std::vector<char>;

    MemBlock(uint64_t pointSize, uint64_t pointsPerBlock)
        : m_pointSize(pointSize)
        , m_pointsPerBlock(pointsPerBlock)
        , m_bytesPerBlock(m_pointsPerBlock * m_pointSize)
    {
        m_blocks.reserve(8);
        m_refs.reserve(m_pointsPerBlock);
    }

    char* next()
    {
        if (m_pos == m_end)
        {
            m_blocks.emplace_back(Block(m_bytesPerBlock));
            m_pos = m_blocks.back().data();
            m_end = m_pos + m_bytesPerBlock;
        }

        char* result(m_pos);
        m_refs.push_back(m_pos);
        m_pos += m_pointSize;
        return result;
    }

    uint64_t size() const { return m_refs.size(); }
    const std::vector<char*>& refs() const { return m_refs; }
    void clear()
    {
        m_blocks.clear();
        m_pos = nullptr;
        m_end = nullptr;
        m_refs.clear();
    }

private:
    const uint64_t m_pointSize;
    const uint64_t m_pointsPerBlock;
    const uint64_t m_bytesPerBlock;

    std::vector<Block> m_blocks;
    char* m_pos = nullptr;
    char* m_end = nullptr;

    std::vector<char*> m_refs;
};

// For writing.
class BlockPointTable : public pdal::SimplePointTable
{
public:
    BlockPointTable(const Schema& schema)
        : SimplePointTable(schema.pdalLayout())
    { }

    void reserve(uint64_t size) { m_refs.reserve(size); }
    void insert(const MemBlock& m)
    {
        m_refs.insert(m_refs.end(), m.refs().begin(), m.refs().end());
    }

    virtual char* getPoint(pdal::PointId index) override
    {
        return m_refs[index];
    }

    virtual pdal::PointId addPoint() override { return m_index++; }
    virtual bool supportsView() const override { return true; }
    uint64_t size() const { return m_refs.size(); }

private:
    std::vector<char*> m_refs;
    uint64_t m_index = 0;
};

// For reading.
class VectorPointTable : public pdal::StreamPointTable
{
    using Process = std::function<void()>;

public:
    VectorPointTable(const Schema& schema, std::size_t np = 4096)
        : pdal::StreamPointTable(schema.pdalLayout(), np)
        , m_pointSize(schema.pointSize())
        , m_data(np * m_pointSize, 0)
    { }

    VectorPointTable(const Schema& schema, std::vector<char>&& data)
        : pdal::StreamPointTable(
                schema.pdalLayout(),
                data.size() / schema.pointSize())
        , m_pointSize(schema.pointSize())
        , m_data(std::move(data))
    {
        if (m_data.size() % m_pointSize != 0)
        {
            throw std::runtime_error("Invalid VectorPointTable data");
        }
    }

    pdal::PointRef at(pdal::PointId index)
    {
        if (index >= capacity())
        {
            throw std::out_of_range("Invalid index to VectorPointTable::at");
        }

        return pdal::PointRef(*this, index);
    }

    virtual char* getPoint(pdal::PointId index) override
    {
        return m_data.data() + index * m_pointSize;
    }

    std::vector<char>& data() { return m_data; }
    const std::vector<char>& data() const { return m_data; }

    std::vector<char>&& acquire() { return std::move(m_data); }

    void setProcess(Process f) { m_f = f; }
    void reset() override { m_f(); }

    class Iterator
    {
    public:
        explicit Iterator(VectorPointTable& table, pdal::PointId index = 0)
            : m_table(table)
            , m_index(index)
            , m_pointRef(m_table, m_index)
        {
            while (m_index < m_table.numPoints() && m_table.skip(m_index))
            {
                ++m_index;
            }
            m_pointRef.setPointId(m_index);
        }

        Iterator& operator++()
        {
            do
            {
                m_pointRef.setPointId(++m_index);
            }
            while (m_index < m_table.numPoints() && m_table.skip(m_index));

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
    Iterator end() { return Iterator(*this, numPoints()); }

    std::size_t pointSize() const { return m_pointSize; }

private:
    VectorPointTable(const VectorPointTable&);
    VectorPointTable& operator=(const VectorPointTable&);

    const std::size_t m_pointSize;
    std::vector<char> m_data;
    std::size_t m_size = 0;

    Process m_f = []() { };
};

} // namespace entwine

