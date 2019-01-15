/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/scan.hpp>

#include <limits>

#include <pdal/StageFactory.hpp>
#include <pdal/util/IStream.hpp>
#include <pdal/util/OStream.hpp>

#include <entwine/builder/thread-pools.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    arbiter::http::Headers rangeHeaders(int start, int end = 0)
    {
        arbiter::http::Headers h;
        h["Range"] = "bytes=" + std::to_string(start) + "-" +
            (end ? std::to_string(end - 1) : "");
        return h;
    }
}

Scan::Scan(const Config config)
    : m_in(merge(Config::defaults(), config.get()))
    , m_arbiter(m_in["arbiter"])
    , m_tmp(m_arbiter.getEndpoint(m_in.tmp()))
    , m_re(m_in.reprojection())
    , m_files(m_in.input())
{
    arbiter::fs::mkdirp(m_tmp.root());
}

Config Scan::go()
{
    if (m_pool || m_done)
    {
        throw std::runtime_error("Cannot call Scan::go twice");
    }
    m_pool = makeUnique<Pool>(m_in.totalThreads(), 1, m_in.verbose());

    const std::size_t size(m_files.size());
    for (std::size_t i(0); i < size; ++i)
    {
        m_index = i;
        FileInfo& f(m_files.get(i));
        if (m_in.verbose())
        {
            std::cout << i + 1 << " / " << size << ": " << f.path() <<
                std::endl;
        }
        add(f);
    }

    m_pool->cycle();

    Config out(aggregate());

    std::string path(m_in.output());
    if (path.size())
    {
        arbiter::Arbiter arbiter(m_in["arbiter"]);
        arbiter::Endpoint ep(arbiter.getEndpoint(path));

        if (ep.isLocal())
        {
            if (!arbiter::fs::mkdirp(ep.root()))
            {
                std::cout << "Could not mkdir: " << path << std::endl;
            }

            if (!arbiter::fs::mkdirp(ep.getSubEndpoint("ept-sources").root()))
            {
                std::cout << "Could not mkdir: " << path << std::endl;
            }
        }

        if (m_in.verbose())
        {
            std::cout << std::endl;
            std::cout << "Writing details to " << path << "..." << std::flush;
        }

        m_files.save(ep, "", m_in, true);
        json j(jsoncppToMjson(out.get()));
        j.erase("input");
        ep.put("scan.json", j.dump(2));

        if (m_in.verbose())
        {
            std::cout << " written." << std::endl;
        }
    }

    return out;
}

void Scan::add(FileInfo& f)
{
    if (!Executor::get().good(f.path())) return;

    m_pool->add([this, &f]()
    {
        if (m_in.trustHeaders() && m_arbiter.isHttpDerived(f.path()))
        {
            const std::string driver = pdal::StageFactory::inferReaderDriver(
                    f.path());

            if (driver == "readers.las") addLas(f);
            else addRanged(f);
        }
        else
        {
            auto localHandle(m_arbiter.getLocalHandle(f.path(), m_tmp));
            add(f, localHandle->localPath());
        }
    });
}

void Scan::addLas(FileInfo& f)
{
    const uint64_t maxHeaderSize(375);

    const uint64_t minorVersionPos(25);
    const uint64_t headerSizePos(94);
    const uint64_t pointOffsetPos(96);
    const uint64_t evlrOffsetPos(235);

    uint8_t minorVersion(0);
    uint16_t headerSize(0);
    uint32_t pointOffset(0);
    uint64_t evlrOffset(0);

    std::string header(m_arbiter.get(f.path(), rangeHeaders(0, maxHeaderSize)));

    std::stringstream headerStream(
            header,
            std::ios_base::in | std::ios_base::out | std::ios_base::binary);

    pdal::ILeStream is(&headerStream);
    pdal::OLeStream os(&headerStream);

    is.seek(minorVersionPos);
    is >> minorVersion;

    is.seek(headerSizePos);
    is >> headerSize;

    is.seek(pointOffsetPos);
    is >> pointOffset;

    if (minorVersion >= 4)
    {
        is.seek(evlrOffsetPos);
        is >> evlrOffset;

        // Modify the header such that the EVLRs come directly after the VLRs -
        // removing the point data itself.
        os.seek(evlrOffsetPos);
        os << pointOffset;
    }

    // Extract the modified header, VLRs, and append the EVLRs.
    header = headerStream.str();
    std::vector<char> data(header.data(), header.data() + headerSize);

    const auto vlrs = m_arbiter.getBinary(
            f.path(),
            rangeHeaders(headerSize, pointOffset));
    data.insert(data.end(), vlrs.begin(), vlrs.end());

    if (minorVersion >= 4)
    {
        const auto evlrs = m_arbiter.getBinary(
                f.path(),
                rangeHeaders(evlrOffset));
        data.insert(data.end(), evlrs.begin(), evlrs.end());
    }

    const std::string ext(arbiter::Arbiter::getExtension(f.path()));
    const std::string basename(
            arbiter::crypto::encodeAsHex(arbiter::crypto::sha256(
                    arbiter::Arbiter::stripExtension(f.path()))) +
            (ext.size() ? "." + ext : ""));

    m_tmp.put(basename, data);
    add(f, m_tmp.fullPath(basename));
    arbiter::fs::remove(m_tmp.fullPath(basename));
}

void Scan::addRanged(FileInfo& f)
{
    const auto data = m_arbiter.getBinary(f.path(), rangeHeaders(0, 16384));

    const std::string ext(arbiter::Arbiter::getExtension(f.path()));
    const std::string basename(
            arbiter::crypto::encodeAsHex(arbiter::crypto::sha256(
                    arbiter::Arbiter::stripExtension(f.path()))) +
            (ext.size() ? "." + ext : ""));

    m_tmp.put(basename, data);
    add(f, m_tmp.fullPath(basename));
    arbiter::fs::remove(m_tmp.fullPath(basename));
}

void Scan::add(FileInfo& f, const std::string localPath)
{
    const json pipeline(jsoncppToMjson(m_in.pipeline(localPath)));

    auto preview(Executor::get().preview(pipeline, m_in.trustHeaders()));
    if (!preview) return;

    f.set(*preview);

    DimList dims;
    for (const std::string name : preview->dimNames) dims.emplace_back(name);

    const Scale scale(preview->scale ? *preview->scale : 1);
    if (!scale.x || !scale.y || !scale.z)
    {
        throw std::runtime_error(
                "Invalid scale " + f.path() + ": " + json(scale).dump());
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_schema = m_schema.merge(Schema(dims));
    m_scale = Point::min(m_scale, scale);
}

Config Scan::aggregate()
{
    Config out;
    Srs srs;

    std::size_t np(0);
    Bounds bounds(Bounds::expander());

    bool explicitSrs(false);
    if (m_re)
    {
        explicitSrs = true;
        srs = m_re->out();
    }
    else if (m_in.get().isMember("srs"))
    {
        explicitSrs = true;
        srs = jsoncppToMjson(m_in.get()["srs"]);
    }

    bool srsLogged(false);
    for (const auto& f : m_files.list())
    {
        if (!f.points()) continue;

        np += f.points();
        if (const Bounds* b = f.bounds()) bounds.grow(*b);

        if (explicitSrs || f.srs().empty()) continue;

        const pdal::SpatialReference& fileSrs(f.srs().ref());

        if (srs.empty())
        {
            if (m_in.verbose())
            {
                std::cout << "Determined SRS from an input file" << std::endl;
            }
            srs = fileSrs.getWKT();
        }
        else if (srs.wkt() != fileSrs.getWKT() && !srsLogged && m_in.verbose())
        {
            srsLogged = true;
            std::cout << "Found potentially conflicting SRS values " <<
                std::endl;
            std::cout << "Setting SRS manually is recommended" << std::endl;
        }
    }

    if (srs.empty() && m_in.verbose())
    {
        std::cout << "SRS could not be determined" << std::endl;
    }

    if (!np) throw std::runtime_error("No points found!");

    if (out["bounds"].isNull()) out["bounds"] = bounds.toJson();

    if (!m_in.absolute())
    {
        if (m_scale == 1) m_scale = 0.01;

        DimInfo x(DimId::X, DimType::Signed32);
        DimInfo y(DimId::Y, DimType::Signed32);
        DimInfo z(DimId::Z, DimType::Signed32);

        x.setScale(m_scale.x);
        y.setScale(m_scale.y);
        z.setScale(m_scale.z);

        DimList dims { x, y, z };
        for (const auto& d : m_schema.dims())
        {
            if (!DimInfo::isXyz(d.id())) dims.emplace_back(d);
        }

        m_schema = Schema(dims);
    }

    if (out["schema"].isNull()) out["schema"] = m_schema.toJson();
    out["points"] = std::max<Json::UInt64>(np, out.points());
    out["input"] = mjsonToJsoncpp(json(m_files));
    if (m_re) out["reprojection"] = mjsonToJsoncpp(json(*m_re));
    out["srs"] = mjsonToJsoncpp(json(srs));
    out["pipeline"] = m_in.pipeline("");

    return out;
}

} // namespace entwine

