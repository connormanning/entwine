#include <cmath>
#include <iostream>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/vector-point-table.hpp>

#include <pdal/BufferReader.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/Writer.hpp>

using namespace entwine;
using D = pdal::Dimension::Id;

namespace
{
    const double pi(std::acos(-1));
    const double cmax(255);

    std::size_t viewIndex(const Point& p)
    {
        return toIntegral(getDirection(Point(), p));
    }
}

int main()
{
    const Point radius(150, 100, 50);

    pdal::PointTable table;
    table.layout()->registerDims({
        D::X, D::Y, D::Z,
        D::Intensity,
        D::ReturnNumber, D::NumberOfReturns,
        D::EdgeOfFlightLine, D::Classification,
        D::ScanAngleRank,
        D::PointSourceId, D::GpsTime,
        D::Red, D::Green, D::Blue,
    });
    table.layout()->finalize();

    std::vector<std::shared_ptr<pdal::PointView>> views;

    for (std::size_t i(0); i < dirEnd(); ++i)
    {
        views.push_back(std::make_shared<pdal::PointView>(table));
    }

    class Mixer
    {
    public:
        Mixer(const Point& p)
        {
            // east/west -> green/magenta
            if (p.x >= 0.0) g = p.x; else m = -p.x;

            // north/south -> red/cyan
            if (p.y >= 0.0) r = p.y; else c = -p.y;

            // up/down -> blue/yellow
            if (p.z >= 0.0) b = p.z; else y = -p.z;
        }

        Color mix() const
        {
            Point p(
                std::max({ r, m, y }),
                std::max({ g, c, y }),
                std::max({ b, c, m }));

            return Color(p.x * cmax, p.y * cmax, p.z * cmax);
        }

        double r = 0, g = 0, b = 0, c = 0, m = 0, y = 0;
    };

    auto addCartesian([&](const Point& p)
    {
        const std::size_t i(viewIndex(p));
        auto& view(*views.at(i));
        const std::size_t n(view.size());
        const double t(
                std::accumulate(
                    views.begin(),
                    views.end(),
                    0,
                    [](std::size_t a, std::shared_ptr<pdal::PointView>& v)
                    {
                        return a + v->size();
                    }));

        view.setField(D::X, n, p.x * radius.x);
        view.setField(D::Y, n, p.y * radius.y);
        view.setField(D::Z, n, p.z * radius.z);

        const Color c(Mixer(p).mix());
        view.setField(D::Red,   n, c.r);
        view.setField(D::Green, n, c.g);
        view.setField(D::Blue,  n, c.b);

        view.setField(D::ReturnNumber, n, p.z >= 0 ? 1 : 2);
        view.setField(D::NumberOfReturns, n, 2);
        view.setField(D::PointSourceId, n, i);
        view.setField(D::GpsTime, n, 42.0 + t * .00001);

        const double xyMag(std::sqrt(p.x * p.x + p.y * p.y));
        view.setField(D::EdgeOfFlightLine, n, xyMag >= 0.95);
        view.setField(D::ScanAngleRank, n, 45.0 * p.x);

        std::size_t cl(0);
        if (xyMag < 0.25) cl = 2;
        else if (xyMag < 0.5) cl = 3;
        else if (xyMag < 0.75) cl = 4;
        else cl = 5;
        if (p.z < 0) cl += 13;
        view.setField(D::Classification, n, cl);

        int s(0); if (p.x >= 0) ++s; if (p.y >= 0) ++s; if (p.z >= 0) ++s;
        bool on = std::abs(s) % 2;
        view.setField(D::Intensity, n, on ? 255 : 128);
    });

    auto addSpherical([&](double t, double r)
    {
        // On unit sphere.
        const Point p(
                std::sin(t) * std::cos(r),
                std::sin(t) * std::sin(r),
                std::cos(t));

        addCartesian(p);
    });

    addCartesian(Point(0, 0, 1));
    addCartesian(Point(0, 1, 0));
    addCartesian(Point(1, 0, 0));
    addCartesian(Point(0, 0, -1));
    addCartesian(Point(0, -1, 0));
    addCartesian(Point(-1, 0, 0));

    {
        double N = 100000 + 4;
        double area = 4 * pi / N;
        double distance = std::sqrt(area);
        double mTheta = std::round(pi / distance);
        double dTheta(pi / mTheta);
        double dPhi(area / dTheta);

        std::size_t total(0);
        for (std::size_t mCount(0); mCount < mTheta; ++mCount)
        {
            double m(mCount);
            double theta = pi * (m + 0.5) / mTheta;
            double mPhi = std::round(2 * pi * std::sin(theta) / dPhi);

            for (std::size_t nCount(0); nCount < mPhi; ++nCount)
            {
                double n(nCount);
                double phi = 2 * pi * n / mPhi;
                addSpherical(theta, phi);
                ++total;
            }
        }
    }

    std::vector<std::shared_ptr<pdal::PointView>> nycViews;
    {
        // From coordinates 40.6892° N, 74.0445° W
        const Point center(-8242596.036, 4966606.257);

        const auto dmin(std::numeric_limits<double>::lowest());
        const auto dmax(std::numeric_limits<double>::max());
        Bounds written;
        written.set(Point(dmax), Point(dmin));

        for (const auto& view : views)
        {
            std::vector<char> point(view->pointSize(), 0);
            const auto dims(view->dimTypes());

            nycViews.push_back(view->makeNew());
            auto& nycView(nycViews.back());
            nycView->setSpatialReference(pdal::SpatialReference("EPSG:3857"));

            for (std::size_t i(0); i < view->size(); ++i)
            {
                view->getPackedPoint(dims, i, point.data());
                nycView->setPackedPoint(dims, i, point.data());

                const Point p(
                        nycView->getFieldAs<double>(D::X, i) + center.x,
                        nycView->getFieldAs<double>(D::Y, i) + center.y,
                        nycView->getFieldAs<double>(D::Z, i) + center.z);

                written.grow(p);

                nycView->setField(D::X, i, p.x);
                nycView->setField(D::Y, i, p.y);
                nycView->setField(D::Z, i, p.z);
            }
        }
    }

    {
        arbiter::fs::mkdirp("ellipsoid-single-laz");

        pdal::BufferReader reader;
        for (auto v : views) reader.addView(v);

        pdal::StageFactory sf;
        pdal::Writer& writer(
                *dynamic_cast<pdal::Writer*>(
                    sf.createStage("writers.las")));

        pdal::Options options;
        options.add("filename", "ellipsoid-single-laz/ellipsoid.laz");
        options.add("compression", "laszip");
        writer.setOptions(options);
        writer.setInput(reader);
        writer.prepare(table);
        writer.execute(table);
    }

    {
        arbiter::fs::mkdirp("ellipsoid-multi-laz");

        for (std::size_t i(0); i < views.size(); ++i)
        {
            auto v(views.at(i));
            const std::string dir(dirToString(toDir(i)));

            pdal::BufferReader reader;
            reader.addView(v);

            pdal::StageFactory sf;
            pdal::Writer& writer(
                    *dynamic_cast<pdal::Writer*>(
                        sf.createStage("writers.las")));

            pdal::Options options;
            options.add("filename", "ellipsoid-multi-laz/" + dir + ".laz");
            options.add("compression", "laszip");
            writer.setOptions(options);
            writer.setInput(reader);
            writer.prepare(table);
            writer.execute(table);
        }
    }

    {
        arbiter::fs::mkdirp("ellipsoid-multi-bpf");

        for (std::size_t i(0); i < views.size(); ++i)
        {
            auto v(views.at(i));
            const std::string dir(dirToString(toDir(i)));

            pdal::BufferReader reader;
            reader.addView(v);

            pdal::StageFactory sf;
            pdal::Writer& writer(
                    *dynamic_cast<pdal::Writer*>(
                        sf.createStage("writers.bpf")));

            pdal::Options options;
            options.add("filename", "ellipsoid-multi-bpf/" + dir + ".bpf");
            writer.setOptions(options);
            writer.setInput(reader);
            writer.prepare(table);
            writer.execute(table);
        }
    }

    {
        arbiter::fs::mkdirp("ellipsoid-single-nyc");

        pdal::BufferReader reader;
        for (auto v : nycViews) reader.addView(v);

        pdal::StageFactory sf;
        pdal::Writer& writer(
                *dynamic_cast<pdal::Writer*>(
                    sf.createStage("writers.las")));

        pdal::Options options;
        options.add("filename", "ellipsoid-single-nyc/ellipsoid.laz");
        options.add("compression", "laszip");
        writer.setSpatialReference(pdal::SpatialReference("EPSG:3857"));
        writer.setOptions(options);
        writer.setInput(reader);
        writer.prepare(table);
        writer.execute(table);
    }

    {
        arbiter::fs::mkdirp("ellipsoid-multi-nyc");

        for (std::size_t i(0); i < nycViews.size(); ++i)
        {
            auto v(nycViews.at(i));
            const std::string dir(dirToString(toDir(i)));

            pdal::BufferReader reader;
            reader.addView(v);

            pdal::StageFactory sf;
            pdal::Writer& writer(
                    *dynamic_cast<pdal::Writer*>(
                        sf.createStage("writers.las")));

            pdal::Options options;
            options.add("filename", "ellipsoid-multi-nyc/" + dir + ".laz");
            options.add("compression", "laszip");
            writer.setSpatialReference(pdal::SpatialReference("EPSG:3857"));
            writer.setOptions(options);
            writer.setInput(reader);
            writer.prepare(table);
            writer.execute(table);
        }
    }

    {
        arbiter::fs::mkdirp("ellipsoid-single-nyc-wrong-srs");

        pdal::BufferReader reader;
        for (auto v : nycViews) reader.addView(v);

        pdal::StageFactory sf;
        pdal::Writer& writer(
                *dynamic_cast<pdal::Writer*>(
                    sf.createStage("writers.las")));

        pdal::Options options;
        options.add("filename", "ellipsoid-single-nyc-wrong-srs/ellipsoid.laz");
        options.add("compression", "laszip");
        writer.setSpatialReference(pdal::SpatialReference("EPSG:26915"));
        writer.setOptions(options);
        writer.setInput(reader);
        writer.prepare(table);
        writer.execute(table);
    }
}

