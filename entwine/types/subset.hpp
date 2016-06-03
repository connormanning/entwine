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

class Structure;

class Subset
{
public:
    Subset(
            Structure& structure,
            const BBox& bbox,
            std::size_t id,
            std::size_t of);

    Subset(Structure& structure, const BBox& bbox, const Json::Value& json);

    Json::Value toJson() const;

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }
    const BBox& bbox() const { return m_sub; }

    std::string postfix() const { return "-" + std::to_string(m_id); }
    bool primary() const { return !m_id; }

private:
    void split(Structure& structure, const BBox& fullBBox);

    std::size_t m_id;
    std::size_t m_of;

    BBox m_sub;
};

} // namespace entwine

