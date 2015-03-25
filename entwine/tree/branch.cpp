/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branch.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

Branch::Branch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : m_schema(schema)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_indexBegin(calcOffset(m_depthBegin, dimensions))
    , m_indexEnd(calcOffset(m_depthEnd, dimensions))
{ }

Branch::Branch(
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : m_schema(schema)
    , m_depthBegin(meta["depthBegin"].asUInt64())
    , m_depthEnd(meta["depthEnd"].asUInt64())
    , m_indexBegin(calcOffset(m_depthBegin, dimensions))
    , m_indexEnd(calcOffset(m_depthEnd, dimensions))
{ }

Branch::~Branch()
{ }

bool Branch::accepts(const std::size_t index) const
{
    return (index >= m_indexBegin) && (index < m_indexEnd);
}

void Branch::save(const std::string& path, Json::Value& meta)
{
    meta["depthBegin"] = static_cast<Json::UInt64>(m_depthBegin);
    meta["depthEnd"] = static_cast<Json::UInt64>(m_depthEnd);

    saveImpl(path, meta);
}

std::size_t Branch::calcOffset(std::size_t depth, std::size_t dimensions)
{
    std::size_t offset(0);

    for (std::size_t i(0); i < depth; ++i)
    {
        offset = (offset << dimensions) + 1;
    }

    return offset;
}

const Schema& Branch::schema() const
{
    return m_schema;
}

std::size_t Branch::depthBegin() const
{
    return m_depthBegin;
}

std::size_t Branch::depthEnd() const
{
    return m_depthEnd;
}

std::size_t Branch::indexBegin() const
{
    return m_indexBegin;
}

std::size_t Branch::indexEnd() const
{
    return m_indexEnd;
}

std::size_t Branch::size() const
{
    return m_indexEnd - m_indexBegin;
}

} //namespace entwine

