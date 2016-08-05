/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/manifest.hpp>

#include <iostream>
#include <limits>

namespace entwine
{

namespace
{
    std::string toString(const FileInfo::Status status)
    {
        switch (status)
        {
            case FileInfo::Status::Outstanding: return "outstanding";
            case FileInfo::Status::Inserted:    return "inserted";
            case FileInfo::Status::Omitted:     return "omitted";
            case FileInfo::Status::Error:       return "error";
            default: throw std::runtime_error("Invalid file info status");
        }
    }

    FileInfo::Status toStatus(const std::string s)
    {
        if (s == "outstanding") return FileInfo::Status::Outstanding;
        if (s == "inserted")    return FileInfo::Status::Inserted;
        if (s == "omitted")     return FileInfo::Status::Omitted;
        if (s == "error")       return FileInfo::Status::Error;
        throw std::runtime_error("Invalid file info status string");
    }

    void error(std::string message)
    {
        throw std::runtime_error(message);
    }
}

PointStats::PointStats(const Json::Value& json)
    : m_inserts(json["inserts"].asUInt64())
    , m_outOfBounds(json["outOfBounds"].asUInt64())
    , m_overflows(json["overflows"].asUInt64())
{ }

Json::Value PointStats::toJson() const
{
    Json::Value json;
    json["inserts"] = static_cast<Json::UInt64>(m_inserts);
    json["outOfBounds"] = static_cast<Json::UInt64>(m_outOfBounds);
    json["overflows"] = static_cast<Json::UInt64>(m_overflows);
    return json;
}

FileStats::FileStats(const Json::Value& json)
    : m_inserts(json["inserts"].asUInt64())
    , m_omits(json["omits"].asUInt64())
    , m_errors(json["errors"].asUInt64())
{ }

Json::Value FileStats::toJson() const
{
    Json::Value json;
    json["inserts"] = static_cast<Json::UInt64>(m_inserts);
    json["omits"] = static_cast<Json::UInt64>(m_omits);
    json["errors"] = static_cast<Json::UInt64>(m_errors);
    return json;
}

FileInfo::FileInfo(const std::string path, const Status status)
    : m_path(path)
    , m_status(status)
    , m_bounds()
    , m_numPoints(0)
    , m_pointStats()
{ }

FileInfo::FileInfo(const FileInfo& other)
    : m_path(other.path())
    , m_status(other.status())
    , m_bounds(other.bounds() ? new Bounds(*other.bounds()) : nullptr)
    , m_numPoints(other.numPoints())
    , m_pointStats(other.pointStats())
{ }

FileInfo& FileInfo::operator=(const FileInfo& other)
{
    m_path = other.path();
    m_status = other.status();
    if (other.bounds()) m_bounds.reset(new Bounds(*other.bounds()));
    m_numPoints = other.numPoints();
    m_pointStats = other.pointStats();

    return *this;
}

FileInfo::FileInfo(const Json::Value& json)
    : m_path(json["path"].asString())
    , m_status(json.isMember("status") ?
            toStatus(json["status"].asString()) : Status::Outstanding)
    , m_bounds(json.isMember("bounds") ? new Bounds(json["bounds"]) : nullptr)
    , m_numPoints(json["numPoints"].asUInt64())
    , m_pointStats(json["pointStats"])
{ }

Json::Value FileInfo::toJson() const
{
    Json::Value json;

    json["path"] = m_path;
    json["status"] = toString(m_status);
    json["pointStats"] = m_pointStats.toJson();

    if (m_bounds) json["bounds"] = m_bounds->toJson();
    if (m_numPoints) json["numPoints"] = static_cast<Json::UInt64>(m_numPoints);

    return json;
}

Json::Value FileInfo::toInferenceJson() const
{
    Json::Value json;

    if (m_bounds)
    {
        Json::Value& bounds(json["bounds"]);

        const Point& min(m_bounds->min());
        const Point& max(m_bounds->max());

        bounds.append(min.x);
        bounds.append(min.y);
        bounds.append(min.z);
        bounds.append(max.x);
        bounds.append(max.y);
        bounds.append(max.z);
    }

    json["path"] = m_path;
    json["numPoints"] = static_cast<Json::UInt64>(m_numPoints);

    return json;
}

Manifest::Manifest(std::vector<std::string> rawPaths)
    : m_paths()
    , m_fileStats()
    , m_pointStats()
    , m_split()
    , m_mutex()
{
    m_paths.reserve(rawPaths.size());
    for (std::size_t i(0); i < rawPaths.size(); ++i)
    {
        m_paths.emplace_back(rawPaths[i]);
    }
}

Manifest::Manifest(const Manifest& other)
    : m_paths(other.m_paths)
    , m_fileStats(other.m_fileStats)
    , m_pointStats(other.m_pointStats)
    , m_split(other.m_split ? new Split(*other.m_split) : nullptr)
    , m_mutex()
{ }

Manifest& Manifest::operator=(const Manifest& other)
{
    m_paths = other.m_paths;
    m_fileStats = other.m_fileStats;
    m_pointStats = other.m_pointStats;
    if (other.m_split) m_split.reset(new Split(*other.m_split));

    return *this;
}

Manifest::Manifest(const Json::Value& json)
    : m_paths()
    , m_fileStats()
    , m_pointStats()
    , m_split(json.isMember("split") ? new Split(json["split"]) : nullptr)
    , m_mutex()
{
    const Json::Value& fileInfo(json["fileInfo"]);
    if (fileInfo.isArray() && fileInfo.size()) m_paths.reserve(fileInfo.size());

    if (json.isMember("fileStats") && json.isMember("pointStats"))
    {
        // Full manifest from Manifest::toJson.
        for (Json::ArrayIndex i(0); i < fileInfo.size(); ++i)
        {
            m_paths.emplace_back(fileInfo[i]);
        }

        m_fileStats = FileStats(json["fileStats"]);
        m_pointStats = PointStats(json["pointStats"]);
    }
    else
    {
        // Simplified inference manifest from Manifest::toInferenceJson.
        for (Json::ArrayIndex i(0); i < fileInfo.size(); ++i)
        {
            m_paths.emplace_back(fileInfo[i]);
        }
    }
}

void Manifest::append(const Manifest& other)
{
    m_paths.reserve(size() + other.size());
    for (const auto& info : other.m_paths)
    {
        countStatus(info.status());
        m_paths.emplace_back(info);
    }
}

void Manifest::merge(const Manifest& other)
{
    if (size() != other.size()) error("Invalid manifest sizes for merging.");

    FileStats fileStats;

    for (std::size_t i(0); i < size(); ++i)
    {
        FileInfo& ours(m_paths[i]);
        const FileInfo& theirs(other.m_paths[i]);

        if (ours.path() != theirs.path()) error("Invalid manifest paths");

        if (
                ours.status() == FileInfo::Status::Outstanding &&
                theirs.status() != FileInfo::Status::Outstanding)
        {
            ours.status(theirs.status());

            switch (theirs.status())
            {
                case FileInfo::Status::Inserted: fileStats.addInsert(); break;
                case FileInfo::Status::Omitted: fileStats.addOmit(); break;
                case FileInfo::Status::Error: fileStats.addError(); break;
                default: throw std::runtime_error("Invalid file status");
            }
        }

        ours.pointStats().add(theirs.pointStats());
    }

    m_pointStats.add(other.pointStats());
    m_fileStats.add(fileStats);
}

Json::Value Manifest::toJson() const
{
    Json::Value json;

    Json::Value& fileInfo(json["fileInfo"]);
    fileInfo.resize(size());

    for (std::size_t i(0); i < size(); ++i)
    {
        const FileInfo& info(m_paths[i]);
        Json::Value& value(fileInfo[static_cast<Json::ArrayIndex>(i)]);

        value = info.toJson();
    }

    json["fileStats"] = m_fileStats.toJson();
    json["pointStats"] = m_pointStats.toJson();

    if (m_split) json["split"] = m_split->toJson();

    return json;
}

Json::Value Manifest::toInferenceJson() const
{
    Json::Value json;

    Json::Value& fileInfo(json["fileInfo"]);
    fileInfo.resize(size());

    for (std::size_t i(0); i < size(); ++i)
    {
        const FileInfo& info(m_paths[i]);
        Json::Value& value(fileInfo[static_cast<Json::ArrayIndex>(i)]);

        value = info.toInferenceJson();
    }

    return json;
}

} // namespace entwine

