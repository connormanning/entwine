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
#include <cassert>
#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/reader/filter.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/stats.hpp>

namespace entwine
{

class Bounds;
class Pool;

class Manifest
{
public:
    Manifest(
            const FileInfoList& fileInfo,
            const arbiter::Endpoint& endpoint);

    Manifest(const Json::Value& json, const arbiter::Endpoint& endpoint);

    Manifest(const Manifest& other);

    void append(const FileInfoList& fileInfo);

    Json::Value toJson() const;
    std::size_t size() const { return m_fileInfo.size(); }

    Origin find(const std::string& path) const;
    OriginList find(const Bounds& bounds) const;
    OriginList find(const Filter& filter) const;

    FileInfo& get(Origin o) { awaken(o); return m_fileInfo.at(o); }
    const FileInfo& get(Origin o) const { awaken(o); return m_fileInfo.at(o); }
    void set(Origin origin, FileInfo::Status status)
    {
        countStatus(status);
        get(origin).status(status);
    }

    void add(Origin origin, const PointStats& stats)
    {
        get(origin).add(stats);

        std::lock_guard<std::mutex> lock(m_mutex);
        m_pointStats.add(stats);
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
        get(origin).pointStats().addOutOfBounds(count);
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

    void save(bool primary, std::string postfix = "") const;

    Paths paths() const
    {
        Paths p;
        p.reserve(m_fileInfo.size());
        for (const auto& f : m_fileInfo) p.push_back(f.path());
        return p;
    }

    void awakenAll(Pool& pool) const;

private:
    void awaken(Origin origin) const;

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

    // Awakening a remote status isn't considered to mutate our state.
    mutable std::vector<FileInfo> m_fileInfo;
    mutable std::vector<bool> m_remote;

    FileStats m_fileStats;
    PointStats m_pointStats;

    const arbiter::Endpoint m_endpoint;
    std::size_t m_chunkSize = 0;

    mutable std::mutex m_mutex;
};

} // namespace entwine

