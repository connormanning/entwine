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

#include <entwine/builder/thread-pools.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/version.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

struct TypedConfig
{
    TypedConfig(const json& j);

    StringList input;
    std::string output;
    std::string tmp;

    json pipeline;
    optional<Bounds> bounds;
    optional<Schema> schema;
    optional<Srs> srs;
    optional<Reprojection> reprojection;

    uint64_t workThreads = 0;
    uint64_t clipThreads = 0;

    std::string dataType;
    uint64_t sleepCount = 0;
    json arbiter;
    bool verbose = true;
    bool stats = true;
    bool force = false;
    bool deep = false;
    uint64_t span = 0;
    uint64_t overflowDepth = 0;
    uint64_t minNodeSize = 0;
    uint64_t maxNodeSize = 0;
    uint64_t cacheSize = 0;
    uint64_t hierarchyStep = 0;
    uint64_t progressInterval = 0;
};

inline void from_json(const json& j, TypedConfig& c) { c = TypedConfig(j); }

class Config
{
public:
    Config() : m_json(json::object()) { }
    Config(const json& j) : m_json(j) { }

    // Used by the builder to normalize the input, this utility ensures:
    //      - Input data is scanned prior to input.
    //      - If the input is already a scan, that the scan is parsed properly
    //        and has its configuration merged in.
    Config prepareForBuild() const;

    json pipeline(std::string filename) const;

    FileInfoList input() const;

    std::string output() const { return m_json.value("output", ""); }
    std::string tmp() const
    {
        return m_json.value("tmp", arbiter::getTempPath());
    }

    uint64_t points() const { return m_json.value("points", 0ull); }
    Schema schema() const { return m_json.value("schema", Schema()); }

    uint64_t totalThreads() const
    {
        uint64_t threads(0);

        const json t(m_json.value("threads", json(8)));
        if (t.is_array())
        {
            threads = t.at(0).get<uint64_t>() + t.at(1).get<uint64_t>();
        }
        else threads = t.get<uint64_t>();

        return std::max<uint64_t>(4u, threads);
    }

    std::size_t workThreads() const
    {
        const json t(m_json.value("threads", json(8)));
        if (t.is_array()) return t.at(0).get<uint64_t>();
        else return ThreadPools::getWorkThreads(t.get<uint64_t>());
    }

    std::size_t clipThreads() const
    {
        const json t(m_json.value("threads", json(8)));
        if (t.is_array()) return t.at(1).get<uint64_t>();
        else return ThreadPools::getClipThreads(t.get<uint64_t>());
    }

    std::string dataType() const
    {
        return m_json.value("dataType", "laszip");
    }
    std::string hierType() const
    {
        return m_json.value("hierarchyType", "json");
    }

    std::unique_ptr<Reprojection> reprojection() const
    {
        return Reprojection::create(m_json.value("reprojection", json()));
    }

    std::size_t sleepCount() const
    {
        return std::max<uint64_t>(
                m_json.value("sleepCount", heuristics::sleepCount),
                500000u);
    }

    bool isContinuation() const
    {
        if (force()) return false;

        arbiter::Arbiter a(arbiter());
        const std::string path = arbiter::join(
                output(), "ept" + postfix() + ".json");
        return !!a.tryGetSize(path);
    }

    std::string arbiter() const
    {
        return m_json.value("arbiter", json()).dump();
    }

    bool verbose() const { return m_json.value("verbose", false); }
    bool force() const { return m_json.value("force", false); }
    bool trustHeaders() const { return m_json.value("trustHeaders", true); }
    bool allowOriginId() const { return m_json.value("allowOriginId", true); }
    uint64_t span() const { return m_json.value("span", 128); }

    uint64_t overflowDepth() const { return m_json.value("overflowDepth", 0); }
    uint64_t minNodeSize() const
    {
        return m_json.value("minNodeSize", span() * span());
    }
    uint64_t maxNodeSize() const
    {
        return m_json.value("maxNodeSize", span() * span() * 4);
    }
    uint64_t cacheSize() const
    {
        return m_json.value("cacheSize", 64);
    }

    uint64_t hierarchyStep() const { return m_json.value("hierarchyStep", 0); }

    Srs srs() const { return m_json.value("srs", Srs()); }

    std::string postfix() const
    {
        if (!m_json.count("subset")) return "";

        return "-" +
            std::to_string(m_json.at("subset").at("id").get<uint64_t>());
    }

    bool absolute() const { return m_json.value("absolute", false); }

    uint64_t progressInterval() const
    {
        return m_json.value("progressInterval", 10);
    }
    uint64_t resetFiles() const
    {
        return m_json.value("resetFiles", 0);
    }

    Bounds bounds() const
    {
        return m_json.value("bounds", Bounds());
    }
    Bounds boundsConforming() const
    {
        return m_json.value("boundsConforming", Bounds());
    }

    void setSubsetId(uint64_t id) { m_json["subset"]["id"] = id; }
    void setSubsetOf(uint64_t of) { m_json["subset"]["of"] = of; }
    void setThreads(uint64_t t) { m_json["threads"] = t; }
    void setInput(const FileInfoList& list) { m_json["input"] = list; }
    void setBounds(const Bounds& b) { m_json["bounds"] = b; }
    void setSchema(const Schema& s) { m_json["schema"] = s; }
    void setPoints(uint64_t p) { m_json["points"] = p; }
    void setReprojection(const Reprojection& r) { m_json["reprojection"] = r; }
    void setSrs(const Srs& s) { m_json["srs"] = s; }
    void setPipeline(json p) { m_json["pipeline"] = p; }

    json subset() const { return m_json.value("subset", json()); }

    const json& toJson() const { return m_json; }

    Version version() const
    {
        return m_json.count("version") ?
            Version(m_json.at("version").get<std::string>()) : Version();
    }

private:
    bool primary() const
    {
        if (!m_json.count("subset")) return true;
        return m_json.at("subset").at("id").get<uint64_t>() == 1u;
    }

    Config fromScan(std::string file) const;

    json m_json;
};

inline void to_json(json& j, const Config& c) { j = c.toJson(); }

} // namespace entwine

