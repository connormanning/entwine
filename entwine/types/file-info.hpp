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

#include <cstddef>
#include <string>
#include <vector>

#include <json/json.h>

#include <pdal/SpatialReference.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/stats.hpp>
#include <entwine/util/executor.hpp>

namespace entwine
{

class FileInfo
{
    friend class Files;

public:
    enum class Status : char
    {
        // Needs insertion.
        Outstanding,

        // No further action needed for this file.  After the build is complete,
        // these should be the only statuses in the metadata.
        Inserted,   // Completed normally - in-bounds points were indexed.
        Omitted,    // Not a point cloud file.
        Error       // An error occurred during insertion.
    };

    explicit FileInfo(std::string path);
    explicit FileInfo(const Json::Value& json);

    // Path from which this file may be read.
    const std::string& path() const         { return m_path; }

    // ID indicating a unique key for this file within source metadata.
    const std::string& id() const           { return m_id; }

    // Source metadata file within which metadata for this file is stored at
    // the key indicated by `id`.
    const std::string& url() const          { return m_url; }

    // These remain constant throughout.
    std::size_t points() const              { return m_points; }
    const Srs& srs() const                  { return m_srs; }
    const Json::Value& metadata() const     { return m_metadata; }
    Origin origin() const                   { return m_origin; }

    const Bounds* bounds() const
    {
        return m_bounds.exists() ? &m_bounds : nullptr;
    }

    const Bounds* boundsEpsilon() const
    {
        return m_bounds.exists() ? &m_boundsEpsilon : nullptr;
    }

    // Status information.
    Status status() const                   { return m_status; }
    const PointStats& pointStats() const    { return m_pointStats; }
    const std::string& message() const      { return m_message; }

    void set(const ScanInfo& scan)
    {
        m_metadata = scan.metadata;

        if (!scan.points) return;

        m_srs = scan.srs;
        m_points = scan.points;
        m_bounds = scan.bounds;
        m_boundsEpsilon = m_bounds.growBy(0.005);
    }

    void setOrigin(uint64_t o) { m_origin = o; }

    void add(const PointStats& stats) { m_pointStats.add(stats); }

    Json::Value toJson() const
    {
        return merge(toListJson(), toFullJson());
    }

    // For use in ept-sources/list.json.
    Json::Value toListJson() const;

    // EPT per-file metadata.
    Json::Value toFullJson() const;

private:
    void setId(std::string id) const { m_id = id; }
    void setUrl(std::string url) const { m_url = url; }
    void add(const FileInfo& other);

    PointStats& pointStats() { return m_pointStats; }
    void status(Status status, std::string message = "")
    {
        m_status = status;
        if (message.size()) m_message = message;
    }

    std::string m_path;
    mutable std::string m_id;
    mutable std::string m_url;
    Status m_status = Status::Outstanding;

    // If Bounds is set while the Status is Outstanding, then we have scanned
    // the bounds and number of points in this file from the header.
    Bounds m_bounds;    // Represented in the output projection.
    Bounds m_boundsEpsilon;
    std::size_t m_points = 0;
    Srs m_srs;
    Json::Value m_metadata;
    Origin m_origin = invalidOrigin;

    PointStats m_pointStats;
    std::string m_message;
};

using FileInfoList = std::vector<FileInfo>;

inline FileInfoList toFileInfo(const Json::Value& json)
{
    FileInfoList f;
    for (const auto& j : json) f.emplace_back(j);
    return f;
}

double densityLowerBound(const FileInfoList& files);
double areaUpperBound(const FileInfoList& files);

} // namespace entwine

