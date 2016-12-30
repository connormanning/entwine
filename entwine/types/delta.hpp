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
    Delta() : m_scale(1), m_offset(0) { }

    Delta(const Scale& scale, const Offset& offset = Offset())
        : m_scale(scale)
        , m_offset(offset)
    { }

    Delta(const Scale* scale, const Offset* offset)
        : m_scale(scale ? *scale : Scale(1))
        , m_offset(offset ? *offset : Offset(0))
    { }

    Delta(const Delta* delta)
        : Delta(
                delta ? delta->scale() : Scale(1),
                delta ? delta->offset() : Offset(0))
    { }

    Delta(const Json::Value& json)
        : Delta()
    {
        if (json.isMember("scale")) m_scale = Scale(json["scale"]);
        if (json.isMember("offset")) m_offset = Scale(json["offset"]);
    }

    void insertInto(Json::Value& json)
    {
        if (empty()) return;

        if (m_scale.x == m_scale.y && m_scale.x == m_scale.z)
        {
            json["scale"] = m_scale.x;
        }
        else json["scale"] = m_scale.toJsonArray();

        json["offset"] = m_offset.toJsonArray();
    }

    static std::unique_ptr<Delta> maybeCreate(
            const Scale* scale,
            const Offset* offset)
    {
        if (scale || offset) return makeUnique<Delta>(scale, offset);
        else return std::unique_ptr<Delta>();
    }

    static std::unique_ptr<Delta> maybeCreate(const Json::Value& json)
    {
        return existsIn(json) ? makeUnique<Delta>(json) : nullptr;
    }

    static bool existsIn(const Json::Value& json)
    {
        return json.isMember("scale") || json.isMember("offset");
    }

    const Scale& scale() const { return m_scale; }
    const Offset& offset() const { return m_offset; }

    Scale& scale() { return m_scale; }
    Offset& offset() { return m_offset; }

    bool empty() const { return m_scale == Scale(1) && m_offset == Offset(0); }
    bool exists() const { return !empty(); }

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

