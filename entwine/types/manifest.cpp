/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/manifest.hpp>

#include <algorithm>
#include <iostream>
#include <limits>

#include <entwine/util/json.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    void error(std::string message)
    {
        throw std::runtime_error(message);
    }

    const std::size_t denseSize(50);
    const std::size_t chunkSize(100);
}

Manifest::Manifest(
        const FileInfoList& fileInfo,
        const arbiter::Endpoint& endpoint)
    : m_fileInfo(fileInfo)
    , m_remote(m_fileInfo.size(), false)
    , m_endpoint(endpoint)
    , m_chunkSize(chunkSize)
{ }

Manifest::Manifest(const Json::Value& json, const arbiter::Endpoint& endpoint)
    : m_endpoint(endpoint)
{
    if (!json.isObject()) throw std::runtime_error("Invalid manifest JSON");

    const Json::Value& fileInfo(json["fileInfo"]);
    if (fileInfo.isArray() && fileInfo.size())
    {
        m_fileInfo.reserve(fileInfo.size());
        for (Json::ArrayIndex i(0); i < fileInfo.size(); ++i)
        {
            m_fileInfo.emplace_back(fileInfo[i]);
        }
    }

    m_chunkSize = json["chunkSize"].asUInt64();
    if (!m_chunkSize) m_chunkSize = chunkSize;

    const bool remote(json["remote"].asBool());
    m_remote.resize(m_fileInfo.size(), remote);

    // If we have fileStats and pointStats, then we're dealing with a full
    // manifest from Manifest::toJson (a previous build).  Otherwise, we have
    // a simplified manifest from Manifest::toInferenceJson.
    if (json.isMember("fileStats") && json.isMember("pointStats"))
    {
        // Full manifest from Manifest::toJson.
        m_fileStats = FileStats(json["fileStats"]);
        m_pointStats = PointStats(json["pointStats"]);
    }
}

Manifest::Manifest(const Manifest& other)
    : m_fileInfo(other.m_fileInfo)
    , m_remote(other.m_remote)
    , m_fileStats(other.m_fileStats)
    , m_pointStats(other.m_pointStats)
    , m_endpoint(other.m_endpoint)
    , m_chunkSize(other.m_chunkSize)
{ }

Origin Manifest::find(const std::string& search) const
{
    for (std::size_t i(0); i < size(); ++i)
    {
        if (m_fileInfo[i].path().find(search) != std::string::npos) return i;
    }

    return invalidOrigin;
}

void Manifest::append(const FileInfoList& fileInfo)
{
    for (const auto& f : fileInfo)
    {
        auto matches([&f](const FileInfo& info)
        {
            return f.path() == info.path();
        });

        if (std::none_of(m_fileInfo.begin(), m_fileInfo.end(), matches))
        {
            m_fileInfo.emplace_back(f);
        }
    }
}

void Manifest::merge(const Manifest& other)
{
    if (size() != other.size()) error("Invalid manifest sizes for merging.");

    FileStats fileStats;

    for (std::size_t i(0); i < size(); ++i)
    {
        FileInfo& ours(m_fileInfo[i]);
        const FileInfo& theirs(other.m_fileInfo[i]);

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
        const FileInfo& info(m_fileInfo[i]);
        Json::Value& value(fileInfo[static_cast<Json::ArrayIndex>(i)]);

        value = info.toJson();
    }

    json["fileStats"] = m_fileStats.toJson();
    json["pointStats"] = m_pointStats.toJson();

    return json;
}

Json::Value Manifest::toInferenceJson() const
{
    Json::Value json;

    Json::Value& fileInfo(json["fileInfo"]);
    fileInfo.resize(size());

    for (std::size_t i(0); i < size(); ++i)
    {
        const FileInfo& info(m_fileInfo[i]);
        Json::Value& value(fileInfo[static_cast<Json::ArrayIndex>(i)]);

        value = info.toInferenceJson();
    }

    return json;
}

void Manifest::awakenAll()
{
    for (std::size_t i(0); i < m_fileInfo.size(); i += m_chunkSize) awaken(i);

    if (std::any_of(
                m_remote.begin(),
                m_remote.end(),
                [](bool b) { return b; }))
    {
        throw std::runtime_error("Invalid remote manifest");
    }
}

void Manifest::awaken(Origin origin) const
{
    if (!m_remote.at(origin)) return;

    const std::size_t chunk(origin / m_chunkSize * m_chunkSize);
    const auto m(m_endpoint.getSubEndpoint("m"));
    const auto bytes(Storage::ensureGet(m, std::to_string(chunk)));
    const auto json(parse(bytes->data()));

    std::size_t i(chunk);
    for (const auto& f : json)
    {
        m_fileInfo.at(i) = FileInfo(f);
        m_remote.at(i) = false;
        ++i;
    }
}

void Manifest::save(const std::string postfix) const
{
    auto m(m_endpoint.getSubEndpoint("m"));

    Json::Value json;
    json["fileStats"] = m_fileStats.toJson();
    json["pointStats"] = m_pointStats.toJson();
    Json::Value& fileInfo(json["fileInfo"]);

    const auto n(size());
    const bool dense(n <= denseSize);
    fileInfo.resize(n);

    if (dense || (m.isLocal() && !arbiter::fs::mkdirp(m.root())))
    {
        for (Json::ArrayIndex i(0); i < n; ++i)
        {
            fileInfo[i] = m_fileInfo[i].toJson();
        }
    }
    else
    {
        for (Json::ArrayIndex i(0); i < n; ++i)
        {
            // We're storing the file info separately, so the "fileInfo" key
            // will just contain string paths instead of the full info object.
            json["remote"] = true;
            json["chunkSize"] = static_cast<uint64_t>(chunkSize);
            fileInfo[i] = m_fileInfo[i].path();
        }

        // TODO Could pool these.
        for (Json::ArrayIndex i(0); i < n; i += chunkSize)
        {
            Json::Value chunk;
            chunk.resize(std::min(chunkSize, n - i));

            for (Json::ArrayIndex c(0); c < chunk.size(); ++c)
            {
                chunk[c] = m_fileInfo[i + c].toJson();
            }

            Storage::ensurePut(m, std::to_string(i), chunk.toStyledString());
        }
    }

    Storage::ensurePut(
            m_endpoint,
            "entwine-manifest" + postfix,
            json.toStyledString());
}

} // namespace entwine

