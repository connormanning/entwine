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

    explicit FileInfo(std::string path, Status status = Status::Outstanding);
    explicit FileInfo(const Json::Value& json);

    // Data required specifically for Entwine indexing - not necessarily related
    // to EPT output itself.
    Json::Value toPrivateJson() const;

    // EPT per-file metadata.
    Json::Value toSourcesJson() const;

    const std::string& path() const         { return m_path; }
    Status status() const                   { return m_status; }
    std::size_t points() const              { return m_points; }
    const Srs& srs() const                  { return m_srs; }
    const PointStats& pointStats() const    { return m_pointStats; }
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

    void set(const ScanInfo& scan)
    {
        m_metadata = scan.metadata;

        if (!scan.points) return;

        m_srs = scan.srs;
        m_points = scan.points;
        m_bounds = scan.bounds;
        m_boundsEpsilon = m_bounds.growBy(0.005);
    }

    void add(const PointStats& stats) { m_pointStats.add(stats); }

private:
    void merge(const FileInfo& b);

    PointStats& pointStats() { return m_pointStats; }
    void status(Status status, std::string message = "")
    {
        m_status = status;
        if (message.size()) m_message = message;
    }

    std::string m_path;
    Status m_status;

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

