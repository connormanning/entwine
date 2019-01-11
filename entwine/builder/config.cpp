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

namespace
{

const std::string scanFile("scan.json");

bool isScan(std::string s)
{
    if (s.size() < scanFile.size()) return false;
    return s.rfind(scanFile) == s.size() - scanFile.size();
}

} // unnamed namespace

Config Config::fromScan(const std::string file) const
{
    if (verbose())
    {
        std::cout << "Using existing scan " << file << std::endl;
    }

    // First grab the configuration portion, which contains things like the
    // pipeline/reprojection used to run this scan, and its results like the
    // scale/schema/SRS/bounds.
    arbiter::Arbiter a(m_json["arbiter"]);
    Config c(entwine::parse(ensureGet(a, file)));

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
    // c["input"] = Files(list).toJson();
    c["input"] = mjsonToJsoncpp(json(list));

    return c;
}

Config Config::prepare() const
{
    Json::Value from(m_json["input"]);

    // For a continuation build, we might just have an output.
    if (from.isNull()) return get();

    // Make sure we have an array.
    if (from.isString())
    {
        const Json::Value prev(from);
        from = Json::arrayValue;
        from.append(prev);
    }

    if (!from.isArray())
    {
        throw std::runtime_error(
                "Unexpected 'input': " + from.toStyledString());
    }

    Json::Value scan;

    // If our input is a Scan, extract it without redoing the scan.
    if (from.size() == 1 && isScan(from[0].asString()))
    {
        scan = fromScan(from[0].asString()).get();
        from = scan["input"];
    }

    if (std::any_of(
                    from.begin(),
                    from.end(),
                    [](Json::Value j) { return !j.isObject(); }))
    {
        // TODO If this is a continued build with files added, we shouldn't be
        // re-scanning everything.  This should be filtered by only the entries
        // which are not objects and then the result intelligently merged.
        if (verbose()) std::cout << "Scanning input" << std::endl;

        // Remove the output from the Scan config - this path is the output
        // path for the subsequent 'build' step.
        Json::Value scanConfig(get());
        Json::Value removed;
        scanConfig.removeMember("output", &removed);
        scan = Scan(scanConfig).go().get();
    }

    // First, soft-merge our scan results over the config without overwriting
    // anything, for example we might have an explicit scale factor or bounds
    // specification that should override scan results.
    Json::Value result = merge(get(), scan, false);

    // If we've just completed a scan or extracted an existing scan, make sure
    // our input represents the scanned data rather than raw paths.
    if (!scan.isNull()) result["input"] = scan["input"];

    // If our input SRS existed, we might potentially overwrite missing fields
    // there with ones we've found from the scan.  Vertical EPSG code, for
    // example.  In this case accept the input without merging.
    if (get().isMember("srs")) result["srs"] = get()["srs"];

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
    else if (result.isMember("scale"))
    {
        s.setScale(Scale(jsoncppToMjson(result["scale"])));
    }
    else if (!s.isScaled())
    {
        s.setScale(Scale(0.01));
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
        for (const auto& c : current)
        {
            if (Executor::get().good(c)) f.emplace_back(c);
        }
    });

    const json& i(jsoncppToMjson(m_json["input"]));
    if (i.is_string()) insert(i);
    else if (i.is_array()) for (const auto& j : i) insert(j);

    return f;
}

Json::Value Config::pipeline(std::string filename) const
{
    const auto r(reprojection());

    Json::Value p(m_json["pipeline"]);
    if (!p.isArray() || p.size() < 1)
    {
        throw std::runtime_error("Invalid pipeline: " + p.toStyledString());
    }

    Json::Value& reader(p[0]);
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
        auto it = std::find_if(p.begin(), p.end(), [](const Json::Value& s)
        {
            return s["type"].asString() == "filters.reprojection";
        });

        Json::Value* repPtr(nullptr);
        if (it != p.end()) repPtr = &*it;
        else repPtr = &p.append(Json::objectValue);

        (*repPtr)["type"] = "filters.reprojection";
        (*repPtr)["out_srs"] = r->out();
    }

    return p;
}

} // namespace entwine

