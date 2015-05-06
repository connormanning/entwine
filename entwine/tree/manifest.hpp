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

#include <cstdint>
#include <set>
#include <string>

#include <entwine/third/json/json.h>

namespace entwine
{

typedef uint64_t Origin;

class Manifest
{
public:
    Manifest();
    explicit Manifest(const Json::Value& meta);

    Json::Value getJson() const;

    // Register a new Origin ID for this path, if this path has not been
    // registered already, in which case invalidOrigin is returned.
    Origin addOrigin(const std::string& path);

    // Note this file as unindexed.
    void addOmission(const std::string& path);

    static Origin invalidOrigin();

private:
    std::vector<std::string> m_originList;
    std::vector<std::string> m_missedList;

    std::set<std::string> m_reverseLookup;
};

} // namespace entwine

