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

#include <memory>
#include <string>
#include <vector>

#include <entwine/types/format-types.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Endpoint; }
namespace cesium { class Settings; }

class Bounds;
class Delta;
class Format;
class Manifest;
class Point;
class Reprojection;
class Schema;
class Structure;
class Subset;

class Metadata
{
    friend class Builder;
    friend class Sequence;

public:
    Metadata(
            const Bounds& boundsNative,
            const Schema& schema,
            const Structure& structure,
            const Structure& hierarchyStructure,
            const Manifest& manifest,
            bool trustHeaders,
            bool compress,
            HierarchyCompression hierarchyCompress,
            const Reprojection* reprojection = nullptr,
            const Subset* subset = nullptr,
            const Delta* delta = nullptr,
            const std::vector<double>* transformation = nullptr,
            const cesium::Settings* cesiumSettings = nullptr);

    Metadata(
            const arbiter::Endpoint& endpoint,
            const std::size_t* subsetId = nullptr);

    explicit Metadata(const Json::Value& json);
    Metadata(const Metadata& other);
    ~Metadata();

    void merge(const Metadata& other);

    void save(const arbiter::Endpoint& endpoint) const;

    const Bounds& boundsNative() const { return *m_boundsNative; }
    const Bounds& boundsConforming() const { return *m_boundsConforming; }
    const Bounds& boundsEpsilon() const { return *m_boundsEpsilon; }
    const Bounds& bounds() const { return *m_bounds; }
    const Bounds* boundsSubset() const;
    const Schema& schema() const { return *m_schema; }
    const Structure& structure() const { return *m_structure; }
    const Structure& hierarchyStructure() const
    {
        return *m_hierarchyStructure;
    }
    const Manifest& manifest() const { return *m_manifest; }
    const Format& format() const { return *m_format; }
    const Reprojection* reprojection() const { return m_reprojection.get(); }
    const Subset* subset() const { return m_subset.get(); }
    const Delta* delta() const { return m_delta.get(); }
    const Transformation* transformation() const
    {
        return m_transformation.get();
    }
    const cesium::Settings* cesiumSettings() const
    {
        return m_cesiumSettings.get();
    }

    const std::vector<std::string>& errors() const { return m_errors; }

    std::string postfix(bool isColdChunk = false) const;
    void makeWhole();

    Json::Value toJson() const;

private:
    Metadata& operator=(const Metadata& other);

    // These are aggregated as the Builder runs.
    Manifest& manifest() { return *m_manifest; }
    Format& format() { return *m_format; }
    std::vector<std::string>& errors() { return m_errors; }

    // The native bounds here is the only one without scale/offset applied.
    std::unique_ptr<Bounds> m_boundsNative;
    std::unique_ptr<Bounds> m_boundsConforming;
    std::unique_ptr<Bounds> m_boundsEpsilon;
    std::unique_ptr<Bounds> m_bounds;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Structure> m_hierarchyStructure;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Delta> m_delta;
    std::unique_ptr<Format> m_format;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Subset> m_subset;
    std::unique_ptr<Transformation> m_transformation;
    std::unique_ptr<cesium::Settings> m_cesiumSettings;

    std::vector<std::string> m_errors;
};

} // namespace entwine

