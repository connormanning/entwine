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
#include <entwine/types/files.hpp>

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

FileInfo::FileInfo(const std::string path)
    : m_path(path)
    , m_id(path)
{
    if (m_path.empty())
    {
        throw std::runtime_error("Empty path found in file-info");
    }
}

FileInfo::FileInfo(const json& j)
    : FileInfo(j.is_object() ?
            j.at("path").get<std::string>() :
            j.get<std::string>())
{
    if (!j.is_object()) return;

    if (j.count("status"))
    {
        m_status = toStatus(j.at("status").get<std::string>());
    }

    m_id = j.value("id", m_id);
    m_url = j.value("url", m_url);
    m_metadata = j.value("metadata", m_metadata);
    m_points = j.value("points", 0);
    m_pointStats = PointStats(j.value("inserts", 0), j.value("outOfBounds", 0));
    m_message = j.value("message", m_message);
    m_srs = j.value("srs", m_srs);
    m_origin = j.value("origin", m_origin);

    if (m_points && j.count("bounds"))
    {
        m_bounds = Bounds(j.at("bounds"));
        m_boundsEpsilon = m_bounds.growBy(0.005);
    }
}

json FileInfo::toListJson() const
{
    /*
    {
        // Public.
        id
        url
        bounds

        // Private.
        path
        status
        points
        inserts
        outOfBounds
        message
    }
    */

    json j;
    j["id"] = m_id;
    j["path"] = m_path;
    if (m_url.size()) j["url"] = m_url;

    if (m_points)
    {
        j["points"] = m_points;
        if (m_bounds.exists()) j["bounds"] = jsoncppToMjson(m_bounds.toJson());
    }

    if (m_status != Status::Outstanding) j["status"] = toString(m_status);
    if (m_pointStats.inserts()) j["inserts"] = m_pointStats.inserts();
    if (m_pointStats.outOfBounds()) j["outOfBounds"] = m_pointStats.oob();
    if (m_message.size()) j["message"] = m_message;

    return j;
}

json FileInfo::toMetaJson() const
{
    /*
    {
        // Public.
        points
        bounds
        srs
        metadata

        // Private.
        origin
    }
    */
    json j(json::object());

    if (m_srs.exists()) j["srs"] = m_srs;
    if (m_bounds.exists()) j["bounds"] = jsoncppToMjson(m_bounds.toJson());

    if (!m_metadata.is_null()) j["metadata"] = m_metadata;
    if (m_origin != invalidOrigin) j["origin"] = m_origin;
    if (m_points) j["points"] = m_points;

    return j;
}

void FileInfo::add(const FileInfo& b)
{
    if (path() != b.path()) throw std::runtime_error("Invalid paths to merge");
    if (m_message.empty() && !b.message().empty()) m_message = b.message();

    if (status() == Status::Outstanding && status() != Status::Outstanding)
    {
        status(b.status());
    }


    add(b.pointStats());
}

double densityLowerBound(const FileInfoList& files)
{
    double points(0);

    for (const auto& f : files)
    {
        if (const auto b = f.bounds())
        {
            if (b->area() > 0 && f.points())
            {
                points += f.points();
            }
        }
    }

    return points / areaUpperBound(files);
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

