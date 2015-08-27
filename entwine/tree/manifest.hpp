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

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <set>
#include <string>

#include <entwine/third/json/json.hpp>

namespace entwine
{

typedef uint64_t Origin;

class Manifest
{
public:
    Manifest();
    explicit Manifest(const Json::Value& meta);

    Json::Value toJson() const;
    Json::Value jsonCounts() const;

    // Register a new Origin ID for this path, if this path has not been
    // registered already, in which case invalidOrigin is returned.
    Origin addOrigin(const std::string& path);

    // Note this file as not indexed.  Primarily intended for insertion of
    // globbed directories, which may contain some non-point-cloud files.
    void addOmission(const std::string& path);

    // Mark a partial insertion of a file that was previously registered with
    // addOrigin() due to an error during insertion.
    void addError(Origin origin);

    static Origin invalidOrigin();

    std::size_t originCount() const { return m_originCount.load(); }
    std::size_t omissionCount() const { return m_omissionCount.load(); }
    std::size_t errorCount() const { return m_errorCount.load(); }

private:
    std::deque<std::string> m_originList;
    std::deque<std::string> m_omissionList;
    std::deque<Origin> m_errorList;

    std::atomic_size_t m_originCount;
    std::atomic_size_t m_omissionCount;
    std::atomic_size_t m_errorCount;

    std::set<std::string> m_reverseLookup;
};

} // namespace entwine

