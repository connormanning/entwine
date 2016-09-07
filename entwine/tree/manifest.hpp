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

#include <algorithm>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include <json/json.h>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>

namespace entwine
{

class PointStats
{
public:
    PointStats() : m_inserts(0), m_outOfBounds(0), m_overflows(0) { }
    explicit PointStats(const Json::Value& json);

    void add(const PointStats& other)
    {
        m_inserts += other.m_inserts;
        m_outOfBounds += other.m_outOfBounds;
        m_overflows += other.m_overflows;
    }

    void addInsert()        { ++m_inserts; }
    void addOutOfBounds()   { ++m_outOfBounds; }
    void addOverflow()      { ++m_overflows; }

    std::size_t inserts() const     { return m_inserts; }
    std::size_t outOfBounds() const { return m_outOfBounds; }
    std::size_t overflows() const   { return m_overflows; }

    void addOutOfBounds(std::size_t n) { m_outOfBounds += n; }
    void clear()
    {
        m_inserts = 0;
        m_outOfBounds = 0;
        m_overflows = 0;
    }

    Json::Value toJson() const;

private:
    std::size_t m_inserts;
    std::size_t m_outOfBounds;
    std::size_t m_overflows;
};

class FileStats
{
public:
    FileStats() : m_inserts(0), m_omits(0), m_errors(0) { }
    explicit FileStats(const Json::Value& json);

    void add(const FileStats& other)
    {
        m_inserts += other.m_inserts;
        m_omits += other.m_omits;
        m_errors += other.m_errors;
    }

    void addInsert()    { ++m_inserts; }
    void addOmit()      { ++m_omits; }
    void addError()     { ++m_errors; }

    Json::Value toJson() const;

private:
    std::size_t m_inserts;
    std::size_t m_omits;
    std::size_t m_errors;
};

typedef std::map<Origin, PointStats> PointStatsMap;

class FileInfo
{
    // Only Manifest may set status.
    friend class Manifest;

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

    FileInfo(std::string path, Status status = Status::Outstanding);
    FileInfo(const FileInfo& other);
    FileInfo& operator=(const FileInfo& other);
    explicit FileInfo(const Json::Value& json);

    Json::Value toJson() const;
    Json::Value toInferenceJson() const;

    const std::string& path() const         { return m_path; }
    Status status() const                   { return m_status; }
    const Bounds* bounds() const            { return m_bounds.get(); }
    const std::size_t numPoints() const     { return m_numPoints; }
    const PointStats& pointStats() const    { return m_pointStats; }

    void bounds(const Bounds& bounds) { m_bounds.reset(new Bounds(bounds)); }
    void numPoints(std::size_t n) { m_numPoints = n; }
    void add(const PointStats& stats) { m_pointStats.add(stats); }

private:
    PointStats& pointStats() { return m_pointStats; }
    void status(Status status) { m_status = status; }

    std::string m_path;
    Status m_status;

    // If Bounds is set while the Status is Outstanding, then we have inferred
    // the bounds and number of points in this file from the header.
    std::unique_ptr<Bounds> m_bounds; // Represented in the output projection.
    std::size_t m_numPoints;

    PointStats m_pointStats;
};

class Manifest
{
public:
    Manifest(std::vector<std::string> paths);
    Manifest(const Manifest& other);
    Manifest& operator=(const Manifest& other);
    explicit Manifest(const Json::Value& meta);

    void append(const Manifest& other);

    Json::Value toJson() const;
    Json::Value toInferenceJson() const;
    std::size_t size() const { return m_paths.size(); }
    const std::vector<FileInfo>& paths() const { return m_paths; }

    FileInfo& get(Origin origin) { return m_paths[origin]; }
    const FileInfo& get(Origin origin) const { return m_paths[origin]; }
    void set(Origin origin, FileInfo::Status status)
    {
        countStatus(status);
        m_paths[origin].status(status);
    }

    void add(Origin origin, const PointStats& stats)
    {
        m_paths[origin].add(stats);

        std::lock_guard<std::mutex> lock(m_mutex);
        m_pointStats.add(stats);
    }

    void clearPointStats()
    {
        for (Origin i(0); i < size(); ++i)
        {
            m_paths[i].pointStats().clear();
        }

        m_pointStats.clear();
    }

    void add(const PointStatsMap& statsMap)
    {
        std::for_each(
                statsMap.begin(),
                statsMap.end(),
                [this](const PointStatsMap::value_type& p)
                {
                    add(p.first, p.second);
                });
    }

    void addOutOfBounds(Origin origin, std::size_t count, bool primary)
    {
        m_paths[origin].pointStats().addOutOfBounds(count);
        if (primary) m_pointStats.addOutOfBounds(count);
    }

    void merge(const Manifest& other);

    FileStats fileStats() const { return m_fileStats; }
    PointStats pointStats() const { return m_pointStats; }

    Json::Value jsonFileStats() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_fileStats.toJson();
    }

    Json::Value jsonPointStats() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_pointStats.toJson();
    }

    bool remote(const arbiter::Arbiter& a) const
    {
        return std::any_of(
                m_paths.begin(),
                m_paths.end(),
                [&a](const FileInfo& f) { return a.isRemote(f.path()); });
    }

private:
    void countStatus(FileInfo::Status status)
    {
        switch (status)
        {
            case FileInfo::Status::Inserted:    m_fileStats.addInsert(); break;
            case FileInfo::Status::Omitted:     m_fileStats.addOmit(); break;
            case FileInfo::Status::Error:       m_fileStats.addError(); break;
            default: break;
        }
    }

    std::vector<FileInfo> m_paths;

    FileStats m_fileStats;
    PointStats m_pointStats;

    mutable std::mutex m_mutex;
};

} // namespace entwine

