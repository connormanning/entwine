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
#include <entwine/types/pooled-point-table.hpp>
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

    const Schema xyzSchema({
        { pdal::Dimension::Id::X },
        { pdal::Dimension::Id::Y },
        { pdal::Dimension::Id::Z }
    });
}

Scan::Scan(const Config config)
    : m_in(config)
    , m_arbiter(config["arbiter"])
    , m_tmp(m_arbiter.getEndpoint(config.tmp()))
    , m_re(config.reprojection())
{ }

Config Scan::go()
{
    if (m_pool || m_done)
    {
        throw std::runtime_error("Cannot call Scan::go twice");
    }
    m_pool = makeUnique<Pool>(m_in.totalThreads());
    m_fileInfo = m_in.input();

    const std::size_t size(m_fileInfo.size());
    for (std::size_t i(0); i < size; ++i)
    {
        m_index = i;
        FileInfo& f(m_fileInfo.at(i));
        std::cout << i + 1 << " / " << size << ": " << f.path() << std::endl;
        add(f);
    }

    m_pool->join();
    return aggregate();
}

void Scan::add(FileInfo& f)
{
    if (Executor::get().good(f.path()))
    {
        if (m_in.trustHeaders() && m_arbiter.isHttpDerived(f.path()))
        {
            m_pool->add([this, &f]()
            {
                const auto data(m_arbiter.getBinary(f.path(), range));

                std::string name(f.path());
                std::replace(name.begin(), name.end(), '/', '-');
                std::replace(name.begin(), name.end(), '\\', '-');

                m_tmp.put(name, data);
                add(f, m_tmp.fullPath(name));
                arbiter::fs::remove(m_tmp.fullPath(name));
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
}

void Scan::add(FileInfo& f, const std::string localPath)
{
    if (auto preview = Executor::get().preview(localPath, m_re.get()))
    {
        f.numPoints(preview->numPoints);
        f.metadata(preview->metadata);
        f.srs(preview->srs);
        if (!preview->numPoints) return;

        f.bounds(preview->bounds);

        DimList dims;
        for (const std::string name : preview->dimNames)
        {
            const pdal::Dimension::Id id(pdal::Dimension::id(name));
            pdal::Dimension::Type t(pdal::Dimension::Type::Double);

            try { t = pdal::Dimension::defaultType(id); }
            catch (pdal::pdal_error&) { }

            dims.emplace_back(name, id, t);
        }

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
}

Config Scan::aggregate()
{
    Config out(m_in);

    std::size_t np(0);
    Bounds bounds(Bounds::expander());

    if (m_re) out["srs"] = m_re->out();

    for (const auto& f : m_fileInfo)
    {
        if (f.numPoints())
        {
            np += f.numPoints();
            if (const Bounds* b = f.bounds()) bounds.grow(*b);
            if (out.srs().empty()) out["srs"] = f.srs().getWKT();
        }
    }

    m_scale = m_scale.apply([](double d)->double
    {
        const double epsilon(1e-6);
        double mult(10);
        while (std::round(d * mult) < 1.0) mult *= 10;

        if (d * mult - 1.0 < epsilon)
        {
            switch (static_cast<int>(mult))
            {
                case int(1e1): return 1e-1;
                case int(1e2): return 1e-2;
                case int(1e3): return 1e-3;
                case int(1e4): return 1e-4;
                case int(1e5): return 1e-5;
                case int(1e6): return 1e-6;
                case int(1e7): return 1e-7;
                case int(1e8): return 1e-8;
                case int(1e9): return 1e-9;
                default: break;
            }
        }
        return d;
    });

    if (out["scale"].isNull()) out["scale"] = m_scale.toJson();
    if (out["bounds"].isNull()) out["bounds"] = bounds.toJson();
    bounds = Bounds(out["bounds"]);

    Offset offset = bounds.mid()
        .apply([](double d) { return std::llround(d); });

    if (std::max(bounds.width(), bounds.depth()) > bounds.height() * 2)
    {
        offset.z = std::llround(bounds.min().z);
    }

    out["offset"] = offset.toJson();

    if (out.scale() != 1)
    {
        DimList dims
        {
            DimInfo(pdal::Dimension::Id::X, pdal::Dimension::Type::Signed32),
            DimInfo(pdal::Dimension::Id::Y, pdal::Dimension::Type::Signed32),
            DimInfo(pdal::Dimension::Id::Z, pdal::Dimension::Type::Signed32)
        };

        for (const auto& d : m_schema.dims())
        {
            if (!DimInfo::isXyz(d.id())) dims.emplace_back(d);
        }

        m_schema = Schema(dims);
    }

    if (out["schema"].isNull()) out["schema"] = m_schema.toJson();
    out["numPoints"] = std::max<Json::UInt64>(np, out.numPoints());
    out["input"] = toJson(m_fileInfo);

    return out;
}

} // namespace entwine

