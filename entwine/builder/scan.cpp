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
    : m_in(merge(Config::defaults(), config.json()))
    , m_arbiter(m_in["arbiter"])
    , m_tmp(m_arbiter.getEndpoint(m_in.tmp()))
    , m_re(m_in.reprojection())
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
    m_fileInfo = m_in.input();

    const std::size_t size(m_fileInfo.size());
    for (std::size_t i(0); i < size; ++i)
    {
        m_index = i;
        FileInfo& f(m_fileInfo.at(i));
        if (m_in.verbose())
        {
            std::cout << i + 1 << " / " << size << ": " << f.path() << std::endl;
        }
        add(f);
    }

    m_pool->join();

    Config out(aggregate());

    std::string path(m_in.output());
    if (path.size())
    {
        if (arbiter::Arbiter::getExtension(path) != "json") path += ".json";

        arbiter::Arbiter arbiter(m_in["arbiter"]);
        if (arbiter.getEndpoint(path).isLocal())
        {
            const auto dir(arbiter::util::getNonBasename(path));
            if (dir.size() && !arbiter::fs::mkdirp(dir))
            {
                std::cout << "Could not mkdir: " <<
                    arbiter::util::getNonBasename(path) << std::endl;
            }
        }

        if (m_in.verbose())
        {
            std::cout << std::endl;
            std::cout << "Writing details to " << path << "...";
        }

        arbiter.put(path,
                m_fileInfo.size() <= 100 ?
                    out.json().toStyledString() : toFastString(out.json()));

        if (m_in.verbose())
        {
            std::cout << " written." << std::endl;
        }
    }

    return out;
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

    std::cout << "TODO Scan::add" << std::endl;
    /*
    if (!m_in.trustHeaders())
    {
        Bounds bounds(Bounds::expander());
        std::size_t np(0);

        auto tracker([&bounds, &np](Cell::PooledStack cells)
        {
            np += cells.size();
            for (const auto& cell : cells) bounds.grow(cell.point());
            return cells;
        });

        const Schema xyz({
            { pdal::Dimension::Id::X },
            { pdal::Dimension::Id::Y },
            { pdal::Dimension::Id::Z }
        });
        PointPool pointPool(xyz);
        PooledPointTable table(pointPool, tracker, invalidOrigin);

        if (Executor::get().run(table, localPath, m_re.get()) && np)
        {
            f.numPoints(np);
            f.bounds(bounds);
        }
    }
    */
}

Config Scan::aggregate()
{
    Config out;

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

    if (!np) throw std::runtime_error("No points found!");

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

    if (out["bounds"].isNull()) out["bounds"] = bounds.toJson();

    if (m_scale != 1 && !m_in.absolute())
    {
        if (m_scale.x == m_scale.y && m_scale.x == m_scale.z)
        {
            out["scale"] = m_scale.x;
        }
        else out["scale"] = m_scale.toJson();
    }

    if (out.delta() && !m_in.absolute())
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
    if (m_re) out["reprojection"] = m_re->toJson();

    return out;
}

} // namespace entwine

