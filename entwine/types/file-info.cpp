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
#include <entwine/types/manifest.hpp>

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
    if (m_path.empty())
    {
        throw std::runtime_error("Empty path found in file-info");
    }

    if (json.isObject())
    {
        if (json.isMember("status"))
        {
            m_status = toStatus(json["status"].asString());
        }

        if (json.isMember("bounds")) bounds(Bounds(json["bounds"]));

        m_numPoints = json["numPoints"].asUInt64();
        m_metadata = json["metadata"];
        m_pointStats = PointStats(json["pointStats"]);
        m_message = json["message"].asString();

        if (json.isMember("srs"))
        {
            m_srs = pdal::SpatialReference(json["srs"].asString());
        }

        if (json.isMember("origin")) m_origin = json["origin"].asUInt64();
    }
}

Json::Value FileInfo::toJson(const bool everything) const
{
    Json::Value json;

    json["path"] = m_path;

    if (m_status != Status::Outstanding) json["status"] = toString(m_status);
    if (!m_pointStats.empty()) json["pointStats"] = m_pointStats.toJson();

    if (everything)
    {
        if (m_origin != invalidOrigin) json["origin"] = (Json::UInt64)m_origin;

        if (m_bounds.exists()) json["bounds"] = m_bounds.toJson();
        if (m_numPoints) json["numPoints"] = (Json::UInt64)m_numPoints;
        if (!m_srs.empty()) json["srs"] = m_srs.getWKT();
        if (!m_message.empty()) json["message"] = m_message;
        if (!m_metadata.isNull()) json["metadata"] = m_metadata;
    }

    return json;
}

double densityLowerBound(const FileInfoList& files)
{
    double points(0);

    for (const auto& f : files)
    {
        if (const auto b = f.bounds())
        {
            if (b->area() > 0 && f.numPoints())
            {
                points += f.numPoints();
            }
        }
    }

    return points / areaUpperBound(files);
}

double densityLowerBound(const Manifest& manifest)
{
    return
        manifest.pointStats().inserts() / areaUpperBound(manifest.fileInfo());
}

double areaUpperBound(const FileInfoList& files)
{
    double area(0);

    for (const auto& f : files)
    {
        if (const auto b = f.bounds())
        {
            if (b->area() > 0) area += b->area();
        }
    }

    return area;
}

} // namespace entwine

