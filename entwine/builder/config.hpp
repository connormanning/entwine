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
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Config
{
public:
    Config() : Config(Json::nullValue) { }
    Config(const Json::Value& json) : m_json(merge(defaults(), json)) { }
    Config prepare() const;

    Json::Value defaults() const
    {
        Json::Value json;

        json["tmp"] = arbiter::fs::getTempPath();
        json["trustHeaders"] = true;
        json["threads"] = 8;

        json["dataType"] = "laszip";
        json["hierarchyType"] = "json";

        json["ticks"] = 256;
        json["overflowDepth"] = 4;
        json["overflowRatio"] = 0.5;

        return json;
    }

    FileInfoList input() const;
    std::string output() const { return m_json["output"].asString(); }
    std::string tmp() const { return m_json["tmp"].asString(); }

    std::size_t numPoints() const { return m_json["numPoints"].asUInt64(); }
    std::size_t totalThreads() const { return m_json["threads"].asUInt64(); }

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
                m_json["sleepCount"].asUInt64(),
                heuristics::sleepCount);
    }

    bool isContinuation() const
    {
        return !force() &&
            arbiter::Arbiter(m_json["arbiter"]).tryGetSize(
                    arbiter::util::join(
                        output(),
                        "entwine" + postfix() + ".json"));
    }

    bool verbose() const { return m_json["verbose"].asBool(); }
    bool force() const { return m_json["force"].asBool(); }
    bool trustHeaders() const { return m_json["trustHeaders"].asBool(); }

    uint64_t ticks() const { return m_json["ticks"].asUInt64(); }
    uint64_t overflowDepth() const { return m_json["overflowDepth"].asUInt64(); }
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

    std::string srs() const { return m_json["srs"].asString(); }
    std::string postfix() const
    {
        if (!m_json["subset"].isNull())
        {
            return "-" + std::to_string(m_json["subset"]["id"].asUInt64());
        }
        return "";
    }

    Scale scale() const
    {
        if (!m_json["scale"].isNull()) return Scale(m_json["scale"]);
        return Scale(0.01);
    }
    Offset offset() const
    {
        if (!m_json["offset"].isNull()) return Offset(m_json["offset"]);
        return Offset(0);
    }
    Delta delta() const { return Delta(scale(), offset()); }

private:
    Json::Value m_json;
};

} // namespace entwine

