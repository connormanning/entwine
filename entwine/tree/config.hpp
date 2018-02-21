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

#include <json/json.h>

namespace entwine
{

class Config
{
public:
    Config(const Json::Value& json);

    std::string input() const { return m_json["input"].asString(); }
    std::string output() const { return m_json["output"].asString(); }
    std::string tmp() const { return m_json["tmp"].asString(); }

    std::size_t threads() const { return m_json["threads"].asUInt64(); }

    std::size_t head() const { return m_json["head"].asUInt64(); }
    std::size_t body() const { return m_json["body"].asUInt64(); }
    std::size_t tail() const { return m_json["tail"].asUInt64(); }

    // bool prefixIds() const { return false; }

    std::string dataStorage() const { return m_json["dataStorage"].asString(); }
    std::string hierStorage() const { return m_json["hierStorage"].asString(); }

    const Json::Value& json() const { return m_json; }
    const Json::Value& operator[](std::string k) const { return m_json[k]; }
    Json::Value& operator[](std::string k) { return m_json[k]; }

private:
    Json::Value defaults() const;

    Json::Value m_json;
};

} // namespace entwine

