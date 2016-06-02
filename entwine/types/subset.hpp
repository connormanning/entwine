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

#include <entwine/types/bbox.hpp>

namespace Json
{
    class Value;
}

namespace entwine
{

class Subset
{
public:
    Subset(const BBox& bbox, std::size_t id, std::size_t of);
    Subset(const BBox& bbox, const Json::Value& json);

    Json::Value toJson() const;

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }
    const BBox& bbox() const { return m_sub; }

    std::string postfix() const { return "-" + std::to_string(m_id); }
    bool primary() const { return !m_id; }

    std::size_t minimumNullDepth() const { return m_minimumNullDepth; }

private:
    void split(const BBox& fullBBox);

    std::size_t m_id;
    std::size_t m_of;

    BBox m_sub;
    std::size_t m_minimumNullDepth;
};

} // namespace entwine

