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

#include <cmath>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>

#include <json/json.h>

#include <entwine/builder/thread-pools.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Config
{
public:
    Config() { }

    Config(const Json::Value& json)
        : m_json(json)
    { }

    Config prepare() const;

    static Json::Value defaults()
    {
        Json::Value json;

        json["tmp"] = arbiter::fs::getTempPath();
        json["trustHeaders"] = true;
        json["threads"] = 8;
        json["pipeline"].append(Json::objectValue);

        return json;
    }

    static Json::Value defaultBuildParams()
    {
        Json::Value json;

        json["dataType"] = "laszip";
        json["hierarchyType"] = "json";

        json["ticks"] = 256;
        json["overflowDepth"] = 0;
        json["overflowRatio"] = 0.5;

        return json;
    }

    Json::Value pipeline(std::string filename) const;

    FileInfoList input() const;
    std::string output() const { return m_json["output"].asString(); }
    std::string tmp() const { return m_json["tmp"].asString(); }

    std::size_t numPoints() const { return m_json["numPoints"].asUInt64(); }
    Schema schema() const { return Schema(m_json["schema"]); }

    std::size_t totalThreads() const
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

    std::string dataType() const { return m_json["dataType"].asString(); }
    std::string hierType() const { return m_json["hierarchyType"].asString(); }

    const Json::Value& json() const { return m_json; }
    Json::Value& json() { return m_json; }
    const Json::Value& operator[](std::string k) const { return m_json[k]; }
    Json::Value& operator[](std::string k) { return m_json[k]; }

    std::unique_ptr<Reprojection> reprojection() const
    {
        return Reprojection::create(m_json["reprojection"]);
    }

    std::size_t sleepCount() const
    {
        return std::max<uint64_t>(
                m_json.isMember("sleepCount") ?
                    m_json["sleepCount"].asUInt64() : heuristics::sleepCount,
                500000);
    }

    bool isContinuation() const
    {
        return !force() &&
            arbiter::Arbiter(m_json["arbiter"]).tryGetSize(
                    arbiter::util::join(
                        output(),
                        "ept" + postfix() + ".json"));
    }

    bool verbose() const { return m_json["verbose"].asBool(); }
    bool force() const { return m_json["force"].asBool(); }
    bool trustHeaders() const { return m_json["trustHeaders"].asBool(); }
    bool allowOriginId() const
    {
        return
            !m_json.isMember("allowOriginId") ||
            m_json["allowOriginId"].asBool();
    }

    uint64_t ticks() const { return m_json["ticks"].asUInt64(); }

    uint64_t overflowDepth() const
    {
        return m_json["overflowDepth"].asUInt64();
    }

    uint64_t overflowThreshold() const
    {
        if (m_json.isMember("overflowThreshold"))
        {
            return m_json["overflowThreshold"].asUInt64();
        }
        else
        {
            return ticks() * ticks() * m_json["overflowRatio"].asDouble();
        }
    }
    uint64_t hierarchyStep() const
    {
        return m_json["hierarchyStep"].asUInt64();
    }

    Srs srs() const
    {
        if (m_json["srs"].isObject()) return Srs(m_json["srs"]);
        else return Srs(m_json["srs"].asString());
    }

    std::string postfix() const
    {
        if (!m_json["subset"].isNull())
        {
            return "-" + std::to_string(m_json["subset"]["id"].asUInt64());
        }
        return "";
    }

    bool absolute() const
    {
        return m_json.isMember("absolute") && m_json["absolute"].asBool();
    }

    uint64_t progressInterval() const
    {
        if (m_json.isMember("progressInterval"))
        {
            return m_json["progressInterval"].asUInt64();
        }
        return 10;
    }

private:
    Json::Value m_json;
};

inline std::ostream& operator<<(std::ostream& os, const Config& c)
{
    os << c.json();
    return os;
}

} // namespace entwine

