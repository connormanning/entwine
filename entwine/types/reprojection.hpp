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

#include <stdexcept>
#include <string>

#include <entwine/third/json/json.hpp>

namespace entwine
{

class Reprojection
{
public:
    Reprojection(std::string in, std::string out, bool hammer = false)
        : m_in(in)
        , m_out(out)
        , m_hammer(hammer)
    {
        check();
    }

    Reprojection(const Json::Value& json)
        : m_in(json["in"].asString())
        , m_out(json["out"].asString())
        , m_hammer(json["hammer"].asBool())
    {
        check();
    }

    Json::Value toJson() const
    {
        Json::Value json;
        json["in"] = in();
        json["out"] = out();
        if (m_hammer) json["hammer"] = true;
        return json;
    }

    std::string in() const { return m_in; }
    std::string out() const { return m_out; }
    bool hammer() const { return m_hammer; }

private:
    void check()
    {
        if (m_out.empty())
        {
            throw std::runtime_error("Empty output projection");
        }

        if (m_hammer && m_in.empty())
        {
            throw std::runtime_error("Hammer option specified without in SRS");
        }
    }

    std::string m_in;
    std::string m_out;

    bool m_hammer;
};

inline std::ostream& operator<<(std::ostream& os, const Reprojection& r)
{
    os << r.in() << " -> " << r.out() << std::endl;
    return os;
}

} // namespace entwine

