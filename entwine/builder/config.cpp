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

#include <cassert>
#include <cmath>

#include <entwine/builder/heuristics.hpp>
#include <entwine/builder/scan.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/exceptions.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{

namespace
{

const std::string scanFile("ept-scan.json");

bool isScan(std::string s)
{
    if (s.size() < scanFile.size()) return false;
    return s.rfind(scanFile) == s.size() - scanFile.size();
}

struct Threads
{
    Threads(uint64_t work, uint64_t clip)
        : work(std::max<uint64_t>(work, 1))
        , clip(std::max<uint64_t>(clip, 3))
    { }

    uint64_t work = 0;
    uint64_t clip = 0;
};

Threads extractThreads(const json& j)
{
    if (j.is_array())
    {
        return Threads(j.at(0).get<uint64_t>(), j.at(1).get<uint64_t>());
    }

    const uint64_t total = j.is_number() ? j.get<uint64_t>() : 8;
    const uint64_t work =
        std::llround(total * heuristics::defaultWorkToClipRatio);
    assert(total >= work);
    const uint64_t clip = total - work;
    return Threads(work, clip);
}

} // unnamed namespace

TypedConfig::TypedConfig(const json& j)
    : input(j.at("input").get<StringList>())
    , output(j.at("output").get<std::string>())
    , tmp(j.value("tmp", arbiter::getTempPath()))
    , pipeline(j.value("pipeline", json::array({ json::object() })))
    , bounds(j.value("bounds", optional<Bounds>()))
    , schema(j.value("schema", optional<Schema>()))
    , srs(j.value("srs", optional<Srs>()))
    , reprojection(j.value("reprojection", optional<Reprojection>()))
    , workThreads(extractThreads(j.value("threads", json())).work)
    , clipThreads(extractThreads(j.value("threads", json())).clip)
    , dataType(j.value("dataType", "laszip"))
    , sleepCount(j.value("sleepCount", heuristics::sleepCount))
    , arbiter(j.value("arbiter", json()))
    , verbose(j.value("verbose", true))
    , stats(j.value("stats", true))
    , force(j.value("force", false))
    , deep(j.value("deep", false))
    , span(j.value("span", 128))
    , progressInterval(j.value("progressInterval", 10))
{
    if (output.empty()) throw ConfigurationError("Missing 'output'")
    if (tmp.empty()) throw ConfigurationError("Missing 'tmp'")
}

Config Config::fromScan(const std::string file) const
{
    if (verbose())
    {
        std::cout << "Using existing scan " << file << std::endl;
    }

    // First grab the configuration portion, which contains things like the
    // pipeline/reprojection used to run this scan, and its results like the
    // scale/schema/SRS/bounds.
    arbiter::Arbiter a(arbiter());
    Config c(json::parse(ensureGet(a, file)));

    // Now we'll pluck out the file information.  Mirroring the EPT source
    // metadata format, we have a sparse list at `ept-sources/list.json` which
    // may point to more detailed metadata information.  Only the primary
    // builder should wake up the detailed metadata to transit it to the final
    // EPT output.
    //
    // The primary builder is a) the sole builder if this is not a subset build
    // or b) the subset with ID 1.
    const std::string dir(file.substr(0, file.rfind(scanFile)));
    arbiter::Endpoint ep(a.getEndpoint(dir));

    FileInfoList list(Files::extract(ep, primary()));
    c.setInput(list);

    return c;
}

Config Config::prepareForBuild() const
{
    if (!m_json.count("output"))
    {
        throw std::runtime_error("Required field 'output' is missing");
    }

    json from(m_json.value("input", json()));

    // For a continuation build, we might just have an output.
    if (from.is_null()) return Config(*this);

    // Make sure we have an array.
    if (from.is_string()) from = json::array({ from });
    if (!from.is_array()) throw std::runtime_error("Bad input: " + from.dump());

    json scan;

    // If our input is a Scan, extract it without redoing the scan.
    if (from.size() == 1 &&
            from.front().is_string() &&
            isScan(from.front().get<std::string>()))
    {
        scan = fromScan(from.front().get<std::string>()).m_json;
        from = scan.at("input");
    }

    if (std::any_of(
                    from.begin(),
                    from.end(),
                    [](json j) { return !j.is_object(); }))
    {
        // TODO If this is a continued build with files added, we shouldn't be
        // re-scanning everything.  This should be filtered by only the entries
        // which are not objects and then the result intelligently merged.
        if (verbose()) std::cout << "Scanning input" << std::endl;

        // Remove the output from the Scan config - this path is the output
        // path for the subsequent 'build' step.
        json scanConfig(m_json);
        scanConfig.erase("output");
        scan = Scan(scanConfig).go().m_json;
    }

    // First, soft-merge our scan results over the config without overwriting
    // anything, for example we might have an explicit scale factor or bounds
    // specification that should override scan results.
    json result = merge(m_json, scan, false);

    // If we've just completed a scan or extracted an existing scan, make sure
    // our input represents the scanned data rather than raw paths.
    if (!scan.is_null()) result["input"] = scan.at("input");

    // If our input SRS existed, we might potentially overwrite missing fields
    // there with ones we've found from the scan.  Vertical EPSG code, for
    // example.  In this case accept the input without merging.
    if (m_json.count("srs")) result["srs"] = m_json["srs"];

    // Prepare the schema, adding OriginId and determining a proper offset, if
    // necessary.
    Schema s(result.value("schema", Schema()));

    if (allowOriginId() && !s.contains(DimId::OriginId))
    {
        s = s.append(DimInfo(DimId::OriginId));
    }

    if (absolute())
    {
        s.setScale(1);
        s.setOffset(0);
    }
    else if (result.count("scale"))
    {
        s.setScale(Scale(result.at("scale")));
    }
    else if (!s.isScaled())
    {
        s.setScale(Scale(0.01));
    }

    if (s.isScaled() && s.offset() == Offset(0))
    {
        const Bounds bounds(result.at("bounds"));
        s.setOffset(bounds.mid().round());
    }

    result["schema"] = s;

    return result;
}

FileInfoList Config::input() const
{
    FileInfoList f;
    arbiter::Arbiter a(arbiter());

    auto insert([&](const json& j)
    {
        if (j.is_object())
        {
            if (Executor::get().good(j.at("path").get<std::string>()))
            {
                f.emplace_back(j);
            }
            return;
        }

        if (!j.is_string())
        {
            throw std::runtime_error(j.dump() + "not convertible to string");
        }

        std::string p(j.get<std::string>());

        if (p.empty()) return;

        if (p.back() != '*')
        {
            if (arbiter::isDirectory(p)) p += '*';
            else if (
                    arbiter::getBasename(p).find_first_of('.') ==
                    std::string::npos)
            {
                p += "/*";
            }
        }

        Paths current(a.resolve(p, verbose()));
        std::sort(current.begin(), current.end());
        for (const auto& c : current)
        {
            if (Executor::get().good(c)) f.emplace_back(c);
        }
    });

    const json i(m_json.value("input", json()));
    if (i.is_string()) insert(i);
    else if (i.is_array()) for (const auto& j : i) insert(j);

    return f;
}

json Config::pipeline(std::string filename) const
{
    const auto r(reprojection());

    json p(m_json.value("pipeline", json::array({ json::object() })));

    if (!p.is_array() || p.empty())
    {
        throw std::runtime_error("Invalid pipeline: " + p.dump(2));
    }

    json& reader(p.at(0));
    if (!filename.empty()) reader["filename"] = filename;

    if (r)
    {
        // First set the input SRS on the reader if necessary.
        if (!r->in().empty())
        {
            if (r->hammer()) reader["override_srs"] = r->in();
            else reader["default_srs"] = r->in();
        }

        // Now set up the output.  If there's already a filters.reprojection in
        // the pipeline, we'll fill it in.  Otherwise, we'll add one to the end.
        auto it = std::find_if(p.begin(), p.end(), [](const json& stage)
        {
            return stage.value("type", "") == "filters.reprojection";
        });

        json* repPtr(nullptr);
        if (it != p.end()) repPtr = &*it;
        else
        {
            p.push_back(json::object());
            repPtr = &p.back();
        }

        (*repPtr)["type"] = "filters.reprojection";
        (*repPtr)["out_srs"] = r->out();
    }

    return p;
}

} // namespace entwine

