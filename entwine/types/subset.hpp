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
#include <memory>

#include <entwine/types/bounds.hpp>

namespace Json
{
    class Value;
}

namespace entwine
{

class Subset
{
public:
    Subset(const Bounds& bounds, std::size_t id, std::size_t of);
    Subset(const Bounds& bounds, const Json::Value& json);

    Json::Value toJson() const;

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }
    const Bounds& bounds() const { return m_sub; }

    std::string postfix() const { return "-" + std::to_string(m_id); }
    bool primary() const { return !m_id; }

    std::size_t minimumNullDepth() const { return m_minimumNullDepth; }
    std::size_t minimumBaseDepth(std::size_t pointsPerChunk) const;

private:
    void split(const Bounds& fullBounds);

    std::size_t m_id;
    std::size_t m_of;

    Bounds m_sub;
    std::size_t m_minimumNullDepth;
};

} // namespace entwine

