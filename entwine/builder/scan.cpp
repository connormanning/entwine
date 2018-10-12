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
    const arbiter::http::Headers range(([]()
    {
        arbiter::http::Headers h;
        h["Range"] = "bytes=0-16384";
        return h;
    })());
}

Scan::Scan(const Config config)
    : m_in(merge(Config::defaults(), config.json()))
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
        }

        if (m_in.verbose())
        {
            std::cout << std::endl;
            std::cout << "Writing details to " << path << "..." << std::flush;
        }

        m_files.writeSources(ep, m_in);
        out["input"] = Files(out["input"]).toPrivateJson();

        const std::string filename("ept-scan.json");
        ep.put(filename, toPreciseString(out.json(), m_files.size() <= 100));

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

    if (m_in.trustHeaders() && m_arbiter.isHttpDerived(f.path()))
    {
        m_pool->add([this, &f]()
        {
            const auto data(m_arbiter.getBinary(f.path(), range));

            const std::string ext(arbiter::Arbiter::getExtension(f.path()));
            const std::string basename(
                    arbiter::crypto::encodeAsHex(arbiter::crypto::sha256(
                            arbiter::Arbiter::stripExtension(f.path()))) +
                    (ext.size() ? "." + ext : ""));

            m_tmp.put(basename, data);
            add(f, m_tmp.fullPath(basename));
            arbiter::fs::remove(m_tmp.fullPath(basename));
        });
    }
    else
    {
        m_pool->add([&f, this]()
        {
            auto localHandle(m_arbiter.getLocalHandle(f.path(), m_tmp));
            add(f, localHandle->localPath());
        });
    }
}

void Scan::add(FileInfo& f, const std::string localPath)
{
    const Json::Value pipeline(m_in.pipeline(localPath));

    auto preview(Executor::get().preview(pipeline, m_in.trustHeaders()));
    if (!preview) return;

    f.set(*preview);

    DimList dims;
    for (const std::string name : preview->dimNames) dims.emplace_back(name);

    const Scale scale(preview->scale ? *preview->scale : 1);
    if (!scale.x || !scale.y || !scale.z)
    {
        throw std::runtime_error(
                "Invalid scale " + f.path() + ": " +
                scale.toJson().toStyledString());
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

    if (m_re)
    {
        srs = m_re->out();
    }
    else if (m_in.json().isMember("srs"))
    {
        const Json::Value& inSrs(m_in.json()["srs"]);
        if (inSrs.isString()) srs = inSrs.asString();
        else srs = inSrs;
    }

    bool srsLogged(false);
    for (const auto& f : m_files.list())
    {
        if (!f.points()) continue;

        np += f.points();
        if (const Bounds* b = f.bounds()) bounds.grow(*b);

        if (f.srs().empty()) continue;

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
    out["input"] = m_files.toSourcesJson();
    if (m_re) out["reprojection"] = m_re->toJson();
    out["srs"] = srs.toJson();
    out["pipeline"] = m_in.pipeline("...");

    return out;
}

} // namespace entwine

