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

#include <entwine/types/bounds.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

void Files::save(const arbiter::Endpoint& ep, const std::string& postfix) const
{
    const Json::Value json(toJson(m_files));
    io::ensurePut(
            ep,
            "entwine-files" + postfix + ".json",
            json.toStyledString());
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

} // namespace entwine

