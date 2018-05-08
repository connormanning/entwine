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
#include <string>

#include <json/json.h>

#include <entwine/tree/thread-pools.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Config
{
public:
    Config(const Json::Value& json);
    Config prepare();

    FileInfoList input() const;
    std::string output() const { return m_json["output"].asString(); }
    std::string tmp() const { return m_json["tmp"].asString(); }

    std::size_t numPoints() const { return m_json["numPoints"].asUInt64(); }

    std::size_t threads() const
    {
        const auto& t(m_json["threads"]);
        if (t.isNumeric()) return t.asUInt64();
        else return t[0].asUInt64() + t[1].asUInt64();
    }
    std::size_t workThreads() const
    {
        const auto& t(m_json["threads"]);
        if (t.isNumeric())
        {
            return ThreadPools::getWorkThreads(m_json["threads"].asUInt64());
        }
        else return t[0].asUInt64();
    }
    std::size_t clipThreads() const
    {
        const auto& t(m_json["threads"]);
        if (t.isNumeric())
        {
            return ThreadPools::getClipThreads(m_json["threads"].asUInt64());
        }
        else return t[1].asUInt64();
    }

    std::size_t head() const { return m_json["head"].asUInt64(); }
    std::size_t body() const { return m_json["body"].asUInt64(); }
    std::size_t tail() const { return m_json["tail"].asUInt64(); }

    std::string dataStorage() const { return m_json["dataStorage"].asString(); }
    std::string hierStorage() const
    {
        return m_json["hierarchyStorage"].asString();
    }

    const Json::Value& json() const { return m_json; }
    const Json::Value& operator[](std::string k) const { return m_json[k]; }
    Json::Value& operator[](std::string k) { return m_json[k]; }

    Scale scale() const { return Scale(m_json["scale"]); }
    Offset offset() const
    {
        if (!m_json["offset"].isNull()) return Offset(m_json["offset"]);
        return boundsConforming().mid().apply(
                [](double d) { return std::llround(d); });
    }
    Delta delta() const { return Delta(scale(), offset()); }

    std::unique_ptr<Reprojection> reprojection() const
    {
        return Reprojection::create(m_json);
    }

    std::size_t sleepCount() const
    {
        return std::max<uint64_t>(
                m_json["sleepCount"].asUInt64(),
                heuristics::sleepCount);
    }

    bool force() const { return m_json["force"].asBool(); }
    bool trustHeaders() const { return m_json["trustHeaders"].asBool(); }
    double density() const { return m_json["density"].asDouble(); }
    std::string srs() const { return m_json["srs"].asString(); }
    std::string postfix() const
    {
        if (!m_json["subset"].isNull())
        {
            return "-" + std::to_string(m_json["subset"]["id"].asUInt64());
        }
        return "";
    }

    Bounds boundsConforming() const
    {
        if (m_json.isMember("boundsConforming"))
        {
            return Bounds(m_json["boundsConforming"]);
        }
        return Bounds(m_json["bounds"]);
    }

private:
    Json::Value defaults() const;

    Json::Value m_json;
};

} // namespace entwine

