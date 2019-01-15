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

#include <entwine/builder/config.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Bounds;
class Pool;

class Files
{
public:
    Files(const FileInfoList& files);
    Files(const json& j) : Files(j.get<FileInfoList>()) { }

    static FileInfoList extract(
            const arbiter::Endpoint& top,
            bool primary,
            std::string postfix = "");

    void save(
            const arbiter::Endpoint& ep,
            const std::string& postfix,
            const Config& config,
            bool primary) const;

    std::size_t size() const { return m_files.size(); }

    Origin find(const std::string& p) const
    {
        for (std::size_t i(0); i < size(); ++i)
        {
            if (m_files[i].path().find(p) != std::string::npos) return i;
        }

        return invalidOrigin;
    }

    FileInfo& get(Origin o) { return m_files.at(o); }
    const FileInfo& get(Origin o) const { return m_files.at(o); }

    void set(Origin o, FileInfo::Status status, std::string message = "")
    {
        get(o).status(status, message);
    }

    void add(Origin origin, const PointStats& stats)
    {
        get(origin).add(stats);

        std::lock_guard<std::mutex> lock(m_mutex);
        m_pointStats.add(stats);
    }

    void addOutOfBounds(Origin origin, std::size_t count, bool primary)
    {
        get(origin).pointStats().addOutOfBounds(count);
        if (primary) m_pointStats.addOutOfBounds(count);
    }

    const FileInfoList& list() const { return m_files; }
    const PointStats& pointStats() const { return m_pointStats; }

    FileInfoList diff(const FileInfoList& fileInfo) const;
    void append(const FileInfoList& fileInfo);

    std::size_t totalPoints() const
    {
        std::size_t n(0);
        for (const auto& f : m_files) n += f.points();
        return n;
    }

    std::size_t totalInserts() const
    {
        std::size_t n(0);
        for (const auto& f : m_files) n += f.pointStats().inserts();
        return n;
    }

    std::size_t totalOutOfBounds() const
    {
        std::size_t n(0);
        for (const auto& f : m_files) n += f.pointStats().outOfBounds();
        return n;
    }

    void merge(const Files& other);

private:
    void writeList(const arbiter::Endpoint& ep, const std::string& postfix)
        const;

    void writeMeta(const arbiter::Endpoint& ep, const Config& config) const;

    FileInfoList m_files;

    mutable std::mutex m_mutex;
    PointStats m_pointStats;
};

inline void to_json(json& j, const Files& f)
{
    j = f.list();
}

} // namespace entwine

