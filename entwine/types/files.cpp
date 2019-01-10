/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/files.hpp>

#include <algorithm>
#include <iostream>
#include <limits>
#include <set>

#include <entwine/io/ensure.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{

std::string idFrom(std::string path)
{
    return arbiter::util::getBasename(path);
}

} // unnamed namespace

Files::Files(const FileInfoList& files)
    : m_files(files)
{
    // Aggregate statuses.
    for (const auto& f : m_files)
    {
        m_pointStats += f.pointStats();
        addStatus(f.status());
    }

    // Initialize origin info for detailed metadata storage purposes.
    for (uint64_t i(0); i < m_files.size(); ++i)
    {
        auto& f(m_files[i]);
        if (f.origin() != i && f.origin() != invalidOrigin)
        {
            throw std::runtime_error("Unexpected origin ID at " +
                    std::to_string(i) + ": " + f.toFullJson().toStyledString());
        }
        f.setOrigin(i);
    }

    // If the basenames of all files are unique amongst one-another, then use
    // the basename as the ID for detailed metadata storage.  Otherwise use the
    // full file path.
    const uint64_t sourcesStep(100);

    bool unique(true);
    std::set<std::string> basenames;
    for (const FileInfo& f : list())
    {
        const auto id(idFrom(f.path()));
        if (basenames.count(id))
        {
            unique = false;
            break;
        }
        else basenames.insert(id);
    }

    if (unique)
    {
        for (uint64_t i(0); i < m_files.size(); ++i)
        {
            const FileInfo& f(m_files[i]);
            f.setId(idFrom(f.path()));
            f.setUrl(std::to_string(i / sourcesStep * sourcesStep) + ".json");
        }
    }
}

FileInfoList Files::extract(
        const arbiter::Endpoint& top,
        const bool primary,
        const std::string postfix)
{
    const auto ep(top.getSubEndpoint("ept-sources"));
    const std::string filename("list" + postfix + ".json");
    FileInfoList list(toFileInfo(parse(ensureGetString(ep, filename))));

    if (!primary) return list;

    std::set<std::string> urls;
    std::map<std::string, Origin> idMap;

    for (Origin i(0); i < list.size(); ++i)
    {
        const FileInfo& f(list[i]);
        if (!f.url().empty()) urls.insert(f.url());
        idMap[f.id()] = i;
    }

    for (const auto url : urls)
    {
        const auto meta(parse(ensureGetString(ep, url)));
        for (const std::string id : meta.getMemberNames())
        {
            const Origin o(idMap.at(id));
            FileInfo& f(list[o]);

            Json::Value current(f.toJson());
            f = FileInfo(entwine::merge(current, meta[id]));
        }
    }

    return list;
}

void Files::save(
        const arbiter::Endpoint& top,
        const std::string& postfix,
        const Config& config,
        const bool detailed) const
{
    const auto ep(top.getSubEndpoint("ept-sources"));
    writeList(ep, postfix);
    if (detailed) writeFull(ep, config);
}

void Files::writeList(
        const arbiter::Endpoint& ep,
        const std::string& postfix) const
{
    Json::Value json;
    for (const FileInfo& f : list()) json.append(f.toListJson());

    const bool styled(size() <= 1000);
    ensurePut(ep, "list" + postfix + ".json", toPreciseString(json, styled));
}

void Files::writeFull(
        const arbiter::Endpoint& ep,
        const Config& config) const
{
    Pool pool(config.totalThreads());

    std::map<std::string, Json::Value> meta;
    for (const auto& f : m_files)
    {
        meta[f.url()][f.id()] = f.toFullJson();
    }

    const bool styled(size() <= 1000);

    for (const auto& p : meta)
    {
        pool.add([this, &ep, &p, styled]()
        {
            const std::string& filename(p.first);
            const Json::Value& json(p.second);
            ensurePut(ep, filename, toPreciseString(json, styled));
        });
    }

    pool.await();
}

void Files::append(const FileInfoList& fileInfo)
{
    FileInfoList adding(diff(fileInfo));
    for (auto& f : adding)
    {
        f.setOrigin(m_files.size());
        m_files.emplace_back(f);
    }
}

FileInfoList Files::diff(const FileInfoList& in) const
{
    FileInfoList out;
    for (const auto& f : in)
    {
        auto matches([&f](const FileInfo& x) { return f.path() == x.path(); });

        if (std::none_of(m_files.begin(), m_files.end(), matches))
        {
            out.emplace_back(f);
        }
    }

    return out;
}

void Files::merge(const Files& other)
{
    if (size() != other.size())
    {
        throw std::runtime_error("Invalid files list for merging");
    }

    for (std::size_t i(0); i < size(); ++i)
    {
        m_files[i].add(other.list()[i]);
    }
}

} // namespace entwine

