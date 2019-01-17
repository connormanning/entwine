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

#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

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
        if (m_out.empty())
        {
            throw std::runtime_error("Empty output projection");
        }

        if (m_hammer && m_in.empty())
        {
            throw std::runtime_error("Hammer option specified without in SRS");
        }
    }

    Reprojection(const json& j)
        : Reprojection(
                j.value("in", ""),
                j.value("out", ""),
                j.value("hammer", false))
    { }

    static std::unique_ptr<Reprojection> create(const json& j)
    {
        if (j.count("out")) return makeUnique<Reprojection>(j);
        else return std::unique_ptr<Reprojection>();
    }

    std::string in() const { return m_in; }
    std::string out() const { return m_out; }
    bool hammer() const { return m_hammer; }

private:
    std::string m_in;
    std::string m_out;

    bool m_hammer = false;
};

inline void to_json(json& j, const Reprojection& r)
{
    j["out"] = r.out();
    if (r.in().size()) j["in"] = r.in();
    if (r.hammer()) j["hammer"] = true;
}

inline std::ostream& operator<<(std::ostream& os, const Reprojection& r)
{
    return os <<
        (r.in().size() ? r.in() : "[headers]") << " " <<
        (r.hammer() ? "(FORCED)" : "(by default)") << " -> " <<
        r.out();
}

} // namespace entwine

