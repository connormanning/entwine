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

#include <entwine/io/ensure.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Json::Value Files::toJson() const
{
    Json::Value json;
    for (const FileInfo& f : list()) json.append(f.toJson());
    return json;
}

void Files::save(
        const arbiter::Endpoint& ep,
        const std::string& postfix,
        const Config& config,
        const bool detailed) const
{
    if (detailed)
    {
        writeDetails(ep.getSubEndpoint("ept-metadata"), postfix, config);
    }

    Json::Value json;
    Json::Value removed;
    for (const FileInfo& f : list())
    {
        Json::Value entry(f.toJson());
        entry.removeMember("metadata", &removed);
        json.append(entry);
    }

    const bool styled(size() <= 1000);
    ensurePut(
            ep,
            "ept-files" + postfix + ".json",
            toPreciseString(json, styled));
}

void Files::writeDetails(
        const arbiter::Endpoint& out,
        const std::string& postfix,
        const Config& config) const
{
    arbiter::Arbiter a(config["arbiter"]);
    std::unique_ptr<arbiter::Endpoint> scanEp;
    if (config["scanDir"].isString())
    {
        scanEp = makeUnique<arbiter::Endpoint>(
                a.getEndpoint(config["scanDir"].asString()));

        if (config.verbose())
        {
            std::cout << "Copying metadata from scan at " << scanEp->root() <<
                std::endl;
        }
    }

    const bool styled(size() <= 1000);
    for (Origin o(0); o < size(); ++o)
    {
        const std::string filename(std::to_string(o) + ".json");

        Json::Value meta;
        if (scanEp) meta = parse(scanEp->get(filename));
        meta = entwine::merge(meta, get(o).metadata());

        ensurePut(
                out,
                filename,
                toPreciseString(meta, styled));
    }
}

void Files::append(const FileInfoList& fileInfo)
{
    FileInfoList adding(diff(fileInfo));
    for (const auto& f : adding) m_files.emplace_back(f);
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
        m_files[i].merge(other.list()[i]);
    }
}

} // namespace entwine

