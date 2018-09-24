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
    Json::Value input;
    Json::Value scanDir;

    // If our input is a Scan, extract it and return the result without redoing
    // the scan.
    Json::Value from(m_json["input"]);

    // Our kernel always builds the input as an array.
    if (from.isArray() && from[0].isString()) from = from[0];

    // For a continuation build, we might just have an output.
    if (from.isNull()) return json();

    if (from.isString())
    {
        const std::string file(from.asString());
        const std::string scanFile("ept-scan.json");
        if (file.rfind(scanFile) == file.size() - scanFile.size())
        {
            if (verbose())
            {
                std::cout << "Using existing scan " << file << std::endl;
            }

            arbiter::Arbiter a(m_json["arbiter"]);
            input = entwine::parse(ensureGet(a, file));
            const std::string dir(file.substr(0, file.rfind(scanFile)));

            scanDir = dir;
        }
    }

    if (input.isNull() && (!from.isObject() || (
                from.isArray() && std::any_of(
                    from.begin(),
                    from.end(),
                    [](Json::Value j) { return !j.isObject(); }))))
    {
        if (verbose()) std::cout << "Scanning input" << std::endl;

        // Remove the output from the Scan config - this path is the output
        // path for the subsequent 'build' step.
        Json::Value scanConfig(json());
        scanConfig.removeMember("output");
        Scan scan(scanConfig);
        Config config(scan.go());
        input = config.json();
    }

    // First, soft-merge our scan results over the config without overwriting
    // anything, for example we might have an explicit scale factor or bounds
    // specification that should override scan results.
    Json::Value result = merge(json(), input, false);

    // Then, always make sure we use the "input" from the scan, which
    // represents the expanded input files and their meta-info rather than the
    // path of the scan or the string paths.
    if (!input.isNull()) result["input"] = input["input"];

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
    result["scanDir"] = scanDir;

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

