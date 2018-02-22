/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <pdal/Filter.hpp>
#include <pdal/Reader.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/unique.hpp>

namespace pdal
{
    class StageFactory;
}

namespace entwine
{

class Reprojection;
class Schema;

class ScopedStage
{
public:
    explicit ScopedStage(
            pdal::Stage* stage,
            pdal::StageFactory& stageFactory,
            std::mutex& factoryMutex);

    ~ScopedStage();

    template<typename T> T getAs() { return dynamic_cast<T>(m_stage); }

private:
    pdal::Stage* m_stage;
    pdal::StageFactory& m_stageFactory;
    std::mutex& m_factoryMutex;
};

typedef std::unique_ptr<ScopedStage> UniqueStage;

class Preview
{
public:
    Preview() = default;

    Preview(
            const Bounds& bounds,
            std::size_t numPoints,
            const std::string& srs,
            const std::vector<std::string>& dimNames,
            const Scale* scale,
            const Json::Value& metadata)
        : bounds(bounds)
        , numPoints(numPoints)
        , srs(srs)
        , dimNames(dimNames)
        , scale(maybeClone(scale))
        , metadata(metadata)
    { }

    Bounds bounds;
    std::size_t numPoints = 0;
    std::string srs;
    std::vector<std::string> dimNames;
    std::unique_ptr<Scale> scale;
    Json::Value metadata;
};

class Executor
{
public:
    static Executor& get()
    {
        static Executor e;
        return e;
    }

    static std::mutex& mutex()
    {
        return get().m_mutex;
    }

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    // Returns true if no errors occurred during insertion.
    template<typename T>
    bool run(
            T& table,
            std::string path,
            const Reprojection* reprojection = nullptr,
            const std::vector<double>* transform = nullptr,
            std::vector<std::string> preserve = std::vector<std::string>());

    // If available, return the bounds specified in the file header without
    // reading the whole file.
    std::unique_ptr<Preview> preview(
            std::string path,
            const Reprojection* reprojection = nullptr);

    std::string getSrsString(std::string input) const;

    Bounds transform(
            const Bounds& bounds,
            const Transformation& transformation) const;

    std::vector<std::string> dims(std::string path) const;

    static std::unique_lock<std::mutex> getLock();

private:
    std::unique_ptr<Preview> slowPreview(
            std::string path,
            const Reprojection* reprojection) const;

    Executor();
    ~Executor();

    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;

    Reprojection srsFoundOrDefault(
            const pdal::SpatialReference& found,
            const Reprojection& given);

    UniqueStage createReader(std::string path) const;
    UniqueStage createFerryFilter(const std::vector<std::string>& s) const;
    UniqueStage createReprojectionFilter(const Reprojection& r) const;
    UniqueStage createTransformationFilter(const std::vector<double>& m) const;

    mutable std::mutex m_mutex;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
};

template<typename T>
bool Executor::run(
        T& table,
        const std::string path,
        const Reprojection* reprojection,
        const std::vector<double>* transform,
        const std::vector<std::string> preserve)
{
    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return false;

    pdal::Reader* reader(scopedReader->getAs<pdal::Reader*>());
    pdal::Stage* executor(reader);

    // Needed so that the SRS has been initialized.
    { auto lock(getLock()); reader->prepare(table); }

    UniqueStage scopedFerry;

    if (preserve.size())
    {
        scopedFerry = createFerryFilter(preserve);
        if (!scopedFerry) return false;

        pdal::Filter* filter(scopedFerry->getAs<pdal::Filter*>());

        filter->setInput(*executor);
        executor = filter;
    }

    UniqueStage scopedReproj;

    if (reprojection)
    {
        const auto srs(
                srsFoundOrDefault(
                    reader->getSpatialReference(), *reprojection));

        scopedReproj = createReprojectionFilter(srs);
        if (!scopedReproj) return false;

        pdal::Filter* filter(scopedReproj->getAs<pdal::Filter*>());

        filter->setInput(*executor);
        executor = filter;
    }

    UniqueStage scopedTransform;

    if (transform)
    {
        scopedTransform = createTransformationFilter(*transform);
        if (!scopedTransform) return false;

        pdal::Filter* filter(scopedTransform->getAs<pdal::Filter*>());

        filter->setInput(*executor);
        executor = filter;
    }

    { auto lock(getLock()); executor->prepare(table); }

    try
    {
        executor->execute(table);
    }
    catch (pdal::pdal_error& e)
    {
        const std::string nostream("Point streaming not supported for stage");
        if (std::string(e.what()).find(nostream) == std::string::npos)
        {
            // If the error was from lack of streaming support, then we'll
            // fall back to the non-streaming API.  Otherwise, return false
            // indicating we couldn't successfully execute this file.
            return false;
        }

        static bool logged(false);
        if (!logged)
        {
            logged = true;
            std::cout <<
                "Streaming execution error - falling back to non-streaming: " <<
                e.what() << std::endl;
        }

        pdal::PointTable pdalTable;
        executor->prepare(pdalTable);
        auto views = executor->execute(pdalTable);

        pdal::PointView pooledView(table);
        auto& layout(*table.layout());
        const auto dimTypes(layout.dimTypes());

        std::vector<char> point(layout.pointSize(), 0);
        char* pos(point.data());

        std::size_t current(0);

        for (auto& view : views)
        {
            for (std::size_t i(0); i < view->size(); ++i)
            {
                view->getPackedPoint(dimTypes, i, pos);
                pooledView.setPackedPoint(dimTypes, current, pos);

                if (++current == table.capacity())
                {
                    table.reset();
                    current = 0;
                }
            }
        }

        if (current) table.reset();
    }

    return true;
}

} // namespace entwine

