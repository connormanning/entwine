/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/file-info.hpp>

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
}

FileInfo::FileInfo(const std::string path, const Status status)
    : m_path(path)
    , m_status(status)
{ }

FileInfo::FileInfo(const Json::Value& json)
    : FileInfo(json.isObject() ? json["path"].asString() : json.asString())
{
    if (json.isObject())
    {
        if (json.isMember("status"))
        {
            m_status = toStatus(json["status"].asString());
        }

        if (json.isMember("bounds")) m_bounds = Bounds(json["bounds"]);

        m_numPoints = json["numPoints"].asUInt64();
        m_metadata = json["metadata"];
        m_pointStats = PointStats(json["pointStats"]);
    }
}

Json::Value FileInfo::toJson() const
{
    Json::Value json(toInferenceJson());

    json["status"] = toString(m_status);
    json["pointStats"] = m_pointStats.toJson();

    return json;
}

Json::Value FileInfo::toInferenceJson() const
{
    Json::Value json;

    json["path"] = m_path;

    if (m_bounds.exists()) json["bounds"] = m_bounds.toJson();
    if (m_numPoints) json["numPoints"] = static_cast<uint64_t>(m_numPoints);
    if (!m_srs.empty()) json["srs"] = m_srs.getWKT();
    if (!m_metadata.isNull()) json["metadata"] = m_metadata;

    return json;
}

} // namespace entwine

