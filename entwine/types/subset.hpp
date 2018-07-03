/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include <json/json.h>

#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>

namespace entwine
{

class Metadata;

class Subset
{
public:
    Subset(const Metadata& metadata, const Json::Value& json);

    static std::unique_ptr<Subset> create(
            const Metadata& metadata,
            const Json::Value& json);

    uint64_t id() const { return m_id; }
    uint64_t of() const { return m_of; }
    uint64_t splits() const { return m_splits; }

    bool primary() const { return m_id == 1; }

    const Bounds& boundsNative() const { return m_boundsNative; }
    const Bounds& boundsScaled() const { return m_boundsScaled; }

    Json::Value toJson() const
    {
        Json::Value json;
        json["id"] = (Json::UInt64)m_id;
        json["of"] = (Json::UInt64)m_of;
        return json;
    }

private:
    const uint64_t m_id;
    const uint64_t m_of;

    const uint64_t m_splits;
    Bounds m_boundsNative;
    Bounds m_boundsScaled;
};

} // namespace entwine

