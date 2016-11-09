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
#include <entwine/util/unique.hpp>

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

    Delta(const Scale* scale, const Offset* offset)
        : m_scale(scale ? *scale : Point(1, 1, 1))
        , m_offset(offset ? *offset : Point(0, 0, 0))
    { }

    Delta(const Json::Value& json)
        : Delta()
    {
        if (json.isMember("scale")) m_scale = Scale(json["scale"]);
        if (json.isMember("offset")) m_offset = Scale(json["offset"]);
    }

    static std::unique_ptr<Delta> maybeCreate(
            const Scale* scale,
            const Offset* offset)
    {
        if (scale || offset) return makeUnique<Delta>(scale, offset);
        else return std::unique_ptr<Delta>();
    }

    static bool existsIn(const Json::Value& json)
    {
        return json.isMember("scale") || json.isMember("offset");
    }

    const Scale& scale() const { return m_scale; }
    const Offset& offset() const { return m_offset; }

    Scale& scale() { return m_scale; }
    Offset& offset() { return m_offset; }

    Delta inverse() const
    {
        return Delta(
                Scale(1.0 / m_scale.x, 1.0 / m_scale.y, 1.0 / m_scale.z),
                -m_offset);
    }

private:
    Scale m_scale;
    Offset m_offset;
};

} // namespace entwine

