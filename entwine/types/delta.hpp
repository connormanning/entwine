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

#include <json/json.h>

#include <entwine/types/defs.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

class Delta
{
public:
    Delta() : m_scale(1, 1, 1), m_offset(0, 0, 0) { }

    Delta(const Scale& scale, const Offset& offset)
        : m_scale(scale)
        , m_offset(offset)
    { }

    Delta(const Json::Value& json)
        : Delta()
    {
        if (json.isMember("scale")) m_scale = Scale(json["scale"]);
        if (json.isMember("offset")) m_offset = Scale(json["offset"]);
    }

    static bool existsIn(const Json::Value& json)
    {
        return json.isMember("scale") || json.isMember("offset");
    }

    const Scale& scale() const { return m_scale; }
    const Offset& offset() const { return m_offset; }

    Scale& scale() { return m_scale; }
    Offset& offset() { return m_offset; }

private:
    Scale m_scale;
    Offset m_offset;
};

} // namespace entwine

