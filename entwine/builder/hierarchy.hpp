/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <map>
#include <mutex>

#include <entwine/types/key.hpp>

namespace entwine
{

class Hierarchy
{
public:
    Hierarchy() { }
    Hierarchy(const Json::Value& json)
    {
        for (const auto key : json.getMemberNames())
        {
            m_map[Dxyz(key)] = json[key].asUInt64();
        }
    }

    void set(const Dxyz& key, uint64_t val)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it(m_map.find(key));
        if (it == m_map.end()) m_map[key] = val;
        else it->second = val;
    }

    uint64_t get(const Dxyz& key) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it(m_map.find(key));
        if (it == m_map.end()) return 0;
        else return it->second;
    }

    Json::Value toJson() const
    {
        Json::Value json;
        for (const auto& p : m_map)
        {
            json[p.first.toString()] = (Json::UInt64)p.second;
        }
        return json;
    }

    using Map = std::map<Dxyz, uint64_t>;
    const Map& map() const { return m_map; }

private:
    mutable std::mutex m_mutex;
    Map m_map;
};

} // namespace entwine

