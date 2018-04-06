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

#include <pdal/Dimension.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/storage-types.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Endpoint; }
namespace cesium { class Settings; }

class Delta;
class Manifest;
class Point;
class Reprojection;
class Schema;
class Storage;
class Structure;
class Subset;
class Version;

class Metadata
{
    friend class Builder;
    friend class Sequence;

public:
    Metadata(
            const Bounds& nativeBounds,
            const Schema& schema,
            const Structure& structure,
            const Structure& hierarchyStructure,
            const Manifest& manifest,
            bool trustHeaders,
            ChunkStorageType chunkStorage,
            HierarchyCompression hierarchyCompress,
            double density,
            const Reprojection* reprojection = nullptr,
            const Subset* subset = nullptr,
            const Delta* delta = nullptr,
            const std::vector<double>* transformation = nullptr,
            const cesium::Settings* cesiumSettings = nullptr,
            std::vector<std::string> preserveSpatial =
                std::vector<std::string>());

    Metadata(const arbiter::Endpoint& endpoint);

    explicit Metadata(const Json::Value& json);
    Metadata(const Metadata& other);
    ~Metadata();

    static std::unique_ptr<Metadata> create(
            const arbiter::Endpoint& endpoint,
            const std::size_t* subsetId = nullptr);

    void merge(const Metadata& other);

    void save(const arbiter::Endpoint& endpoint) const;

    static Bounds makeScaledCube(
            const Bounds& nativeConformingBounds,
            const Delta* delta);

    static Bounds makeNativeCube(
            const Bounds& nativeConformingBounds,
            const Delta* delta);

    // Native bounds - no scale/offset applied.
    const Bounds& boundsNativeConforming() const
    {
        return *m_boundsNativeConforming;
    }
    const Bounds& boundsNativeCubic() const
    {
        return *m_boundsNativeCubic;
    }
    std::unique_ptr<Bounds> boundsNativeSubset() const;

    // Aliases for API simplicity.
    const Bounds& boundsConforming() const { return boundsNativeConforming(); }
    const Bounds& boundsCubic() const { return boundsNativeCubic(); }

    // Scaled bounds - scale/offset applied.
    const Bounds& boundsScaledConforming() const
    {
        return *m_boundsScaledConforming;
    }
    const Bounds& boundsScaledCubic() const
    {
        return *m_boundsScaledCubic;
    }
    const Bounds& boundsScaledEpsilon() const
    {
        return *m_boundsScaledEpsilon;
    }
    std::unique_ptr<Bounds> boundsScaledSubset() const;

    const Schema& schema() const { return *m_schema; }
    const Structure& structure() const { return *m_structure; }
    const Structure& hierarchyStructure() const
    {
        return *m_hierarchyStructure;
    }
    const Manifest* manifestPtr() const { return m_manifest.get(); }
    const Manifest& manifest() const { return *m_manifest; }
    const Storage& storage() const { return *m_storage; }
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
    const std::vector<std::string>& preserveSpatial() const
    {
        return m_preserveSpatial;
    }

    const Version& version() const { return *m_version; }
    const std::string& srs() const { return m_srs; }
    double density() const { return m_density; }
    bool trustHeaders() const { return m_trustHeaders; }
    bool slicedBase() const { return m_slicedBase; }

    std::string basename(const Id& chunkId) const;
    std::string filename(const Id& chunkId) const;
    std::string postfix(bool isColdChunk = false) const;
    void unbump();
    void makeWhole();

    Json::Value toJson() const;

private:
    static Json::Value unify(Json::Value json);

    void awakenManifest(const arbiter::Endpoint& ep);
    Metadata& operator=(const Metadata& other);

    // These are aggregated as the Builder runs.
    Manifest& manifest() { return *m_manifest; }
    Manifest* manifestPtr() { return m_manifest.get(); }
    // Storage& storage() { return *m_storage; }
    std::string& srs() { return m_srs; }

    std::unique_ptr<Delta> m_delta;

    std::unique_ptr<Bounds> m_boundsNativeConforming;
    std::unique_ptr<Bounds> m_boundsNativeCubic;

    std::unique_ptr<Bounds> m_boundsScaledConforming;
    std::unique_ptr<Bounds> m_boundsScaledCubic;
    std::unique_ptr<Bounds> m_boundsScaledEpsilon;

    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Structure> m_hierarchyStructure;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Storage> m_storage;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Subset> m_subset;
    std::unique_ptr<Transformation> m_transformation;
    std::unique_ptr<cesium::Settings> m_cesiumSettings;
    std::unique_ptr<Version> m_version;
    std::string m_srs;
    double m_density = 0;
    bool m_trustHeaders = true;
    bool m_slicedBase = true;
    std::vector<std::string> m_preserveSpatial;
};

} // namespace entwine

