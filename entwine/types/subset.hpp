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

class Subset
{
public:
    Subset(
            std::size_t id,
            std::size_t of,
            std::size_t dimensions,
            const BBox* bbox);

    Subset(const Json::Value& json, const BBox& bbox);
    Subset(const Subset& other);
    Subset& operator=(const Subset& other);

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }

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
    std::size_t m_id;
    std::size_t m_of;

    std::unique_ptr<BBox> m_sub;
};

} // namespace entwine

