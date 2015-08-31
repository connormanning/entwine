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
            const Structure& structure,
            const BBox* bbox,
            std::size_t id,
            std::size_t of);

    Subset(
            const Structure& structure,
            const BBox& bbox,
            const Json::Value& json);

    Subset(const Subset& other);

    Json::Value toJson() const;

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }
    std::size_t minNullDepth() const { return m_minNullDepth; }

    void update(const BBox& fullBBox);

    const BBox& bbox() const
    {
        if (!m_sub)
        {
            throw std::runtime_error("Cannot subset non-existent bounds");
        }

        return *m_sub;
    }

private:
    const Structure& m_structure;

    std::size_t m_id;
    std::size_t m_of;

    std::unique_ptr<BBox> m_sub;
    std::size_t m_minNullDepth;
};

} // namespace entwine

