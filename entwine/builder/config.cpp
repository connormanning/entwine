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
#include <entwine/io/ensure.hpp>
#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

Config Config::prepare() const
{
    Json::Value scan;

    // If our input is a Scan, extract it and return the result without redoing
    // the scan.
    Json::Value p(m_json["input"]);
    if (p.isNull()) return json();
    if (p.isArray() && p.size() == 1) p = p[0];
    if (p.isString() && arbiter::Arbiter::getExtension(p.asString()) == "json")
    {
        if (verbose()) std::cout << "Using existing scan as input" << std::endl;
        const auto path(p.asString());
        arbiter::Arbiter a(m_json["arbiter"]);
        scan = entwine::parse(ensureGet(a, path));
    }
    else if (
            (!p.isObject()) || (
                p.isArray() && std::any_of(
                    p.begin(),
                    p.end(),
                    [](Json::Value j) { return !j.isObject(); })))
    {
        if (verbose()) std::cout << "Scanning input" << std::endl;

        // Remove the output from the Scan config - this path is the output
        // path for the subsequent 'build' step.
        Json::Value scanConfig(json());
        scanConfig.removeMember("output");
        scan = Scan(scanConfig).go().json();
    }

    // First, soft-merge our scan results over the config without overwriting
    // anything, for example we might have an explicit scale factor or bounds
    // specification that should override scan results.
    Json::Value result = merge(json(), scan, false);

    // Then, always make sure we use the "input" from the scan, which
    // represents the expanded input files and their meta-info rather than the
    // path of the scan or the string paths.
    if (!scan.isNull()) result["input"] = scan["input"];

    // Prepare the schema, adding OriginId and determining a proper offset, if
    // necessary.
    Schema s(result["schema"]);

    if (allowOriginId() && !s.contains(DimId::OriginId))
    {
        s = s.append(DimInfo(DimId::OriginId));
    }

    if (absolute())
    {
        s.setScale(1);
        s.setOffset(0);
    }
    else if (!s.isScaled())
    {
        s.setScale(
                result.isMember("scale") ?
                    Scale(result["scale"]) : Scale(0.01));
    }

    if (s.isScaled() && s.offset() == Offset(0))
    {
        const Bounds bounds(result["bounds"]);
        s.setOffset(bounds.mid().round());
    }

    result["schema"] = s.toJson();

    return result;
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

        Paths current(arbiter.resolve(p, verbose()));
        std::sort(current.begin(), current.end());
        for (const auto& c : current) f.emplace_back(c);
    });

    const auto& i(m_json["input"]);
    if (i.isString()) insert(i);
    else if (i.isArray()) for (const auto& j : i) insert(j);

    return f;
}

} // namespace entwine

