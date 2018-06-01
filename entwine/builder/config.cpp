/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/config.hpp>

#include <entwine/builder/scan.hpp>
#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

Config Config::prepare() const
{
    // If our input is a Scan, extract it and return the result without redoing
    // the scan.
    const Json::Value& p(m_json["input"]);
    if (p.isString() && arbiter::Arbiter::getExtension(p.asString()) == "json")
    {
        std::cout << "Using existing scan as input" << std::endl;
        const auto path(p.asString());
        arbiter::Arbiter a(m_json["arbiter"]);
        const auto scan(entwine::parse(a.get(path)));
        return merge(json(), scan);
    }
    else
    {
        Scan scan(*this);
        return merge(json(), scan.go().json());
    }
}

FileInfoList Config::input() const
{
    FileInfoList f;
    arbiter::Arbiter arbiter(m_json["arbiter"]);

    auto insert([&](const Json::Value& j)
    {
        if (j.isObject())
        {
            f.emplace_back(j);
            return;
        }

        if (!j.isString())
        {
            throw std::runtime_error(
                    j.toStyledString() + " not convertible to string");
        }

        std::string p(j.asString());

        if (p.empty()) return;

        if (p.back() != '*')
        {
            if (arbiter::util::isDirectory(p)) p += '*';
            else if (
                    arbiter::util::getBasename(p).find_first_of('.') ==
                    std::string::npos)
            {
                p += "/*";
            }
        }

        Paths current(arbiter.resolve(p, true));
        std::sort(current.begin(), current.end());
        for (const auto& c : current) f.emplace_back(c);
    });

    const auto& i(m_json["input"]);
    if (i.isString()) insert(i);
    else if (i.isArray()) for (const auto& j : i) insert(j);

    return f;
}

} // namespace entwine

