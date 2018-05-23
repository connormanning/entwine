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

#include <entwine/builder/config.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Endpoint; }

class ChunkStorage;
class Delta;
class Files;
class Point;
class Reprojection;
class Schema;
class Subset;
class Version;

class Metadata
{
    friend class Builder;
    friend class Sequence;

public:
    Metadata(const Config& config, bool exists = false);
    Metadata(
            const arbiter::Endpoint& endpoint,
            const Config& config = Config());

    ~Metadata();

    void merge(const Metadata& other);
    void save(const arbiter::Endpoint& endpoint) const;

public:
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
    std::unique_ptr<Bounds> boundsScaledSubset() const;

    const Schema& schema() const { return *m_schema; }
    const Files& files() const { return *m_files; }

    std::string chunkStorageType() const { return "laszip"; }   // TODO
    const ChunkStorage& storage() const { return *m_chunkStorage; }

    const Reprojection* reprojection() const { return m_reprojection.get(); }
    const Subset* subset() const { return m_subset.get(); }
    const Delta* delta() const { return m_delta.get(); }
    const Transformation* transformation() const
    {
        return m_transformation.get();
    }

    const Version& version() const { return *m_version; }
    const std::string& srs() const { return m_srs; }

    bool trustHeaders() const { return m_trustHeaders; }

    uint64_t totalPoints() const { return m_totalPoints; }
    uint64_t ticks() const { return m_ticks; }
    uint64_t startDepth() const { return m_startDepth; }
    uint64_t sharedDepth() const { return m_sharedDepth; }
    uint64_t overflowDepth() const { return m_overflowDepth; }
    uint64_t overflowThreshold() const { return m_overflowThreshold; }

    void makeWhole();

    std::string postfix() const;
    std::string postfix(uint64_t depth) const;

private:
    Json::Value toJson() const;
    Json::Value toBuildParamsJson() const;

    Metadata& operator=(const Metadata& other);

    Bounds makeNativeConformingBounds(const Bounds& b) const;
    Bounds makeNativeCube(const Bounds& b, const Delta& d) const;
    /*
    Bounds makeScaledConformingBounds(const Bounds& b, const Delta& d) const;
    Bounds makeScaledCube(const Bounds& b, const Delta& d) const;
    */

    // These are aggregated as the Builder runs.
    // Manifest& manifest() { return *m_manifest; }
    Files& mutableFiles() { return *m_files; }
    // ChunkStorage& storage() { return *m_chunkStorage; }
    std::string& srs() { return m_srs; }

    std::unique_ptr<Delta> m_delta;

    std::unique_ptr<Bounds> m_boundsNativeConforming;
    std::unique_ptr<Bounds> m_boundsNativeCubic;

    std::unique_ptr<Bounds> m_boundsScaledConforming;
    std::unique_ptr<Bounds> m_boundsScaledCubic;

    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Files> m_files;
    std::unique_ptr<ChunkStorage> m_chunkStorage;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Transformation> m_transformation;
    std::unique_ptr<Version> m_version;
    std::string m_srs;
    std::unique_ptr<Subset> m_subset;

    const bool m_trustHeaders = true;
    const uint64_t m_totalPoints;

    const uint64_t m_ticks;
    const uint64_t m_startDepth;
    const uint64_t m_sharedDepth;

    const uint64_t m_overflowDepth;
    const uint64_t m_overflowThreshold;
};

} // namespace entwine

