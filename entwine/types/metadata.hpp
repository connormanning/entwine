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
#include <entwine/types/subset.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Endpoint; }

class DataIo;
class Files;
class Point;
class Reprojection;
class Schema;
class Srs;
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
    void save(const arbiter::Endpoint& endpoint, const Config& config) const;

    const Bounds& boundsConforming() const { return *m_boundsConforming; }
    const Bounds& boundsCubic() const { return *m_boundsCubic; }
    const Bounds* boundsSubset() const
    {
        if (m_subset) return &m_subset->bounds();
        else return nullptr;
    }

    const Schema& schema() const { return *m_schema; }
    const Schema& outSchema() const { return *m_outSchema; }
    const Files& files() const { return *m_files; }

    const DataIo& dataIo() const { return *m_dataIo; }

    const Reprojection* reprojection() const { return m_reprojection.get(); }
    const Subset* subset() const { return m_subset.get(); }

    const Version& version() const { return *m_version; }
    const Srs& srs() const { return *m_srs; }

    bool trustHeaders() const { return m_trustHeaders; }
    bool primary() const { return !m_subset || m_subset->primary(); }

    uint64_t ticks() const { return m_ticks; }
    uint64_t startDepth() const { return m_startDepth; }
    uint64_t sharedDepth() const { return m_sharedDepth; }
    uint64_t overflowDepth() const { return m_overflowDepth; }
    uint64_t overflowThreshold() const { return m_overflowThreshold; }
    uint64_t hierarchyStep() const { return m_hierarchyStep; }

    void makeWhole();

    std::string postfix() const;
    std::string postfix(uint64_t depth) const;

    Json::Value toJson() const;
    Json::Value toBuildParamsJson() const;

private:
    Metadata& operator=(const Metadata& other);
    void setHierarchyStep(uint64_t v) { m_hierarchyStep = v; }

    Bounds makeConformingBounds(Bounds b) const;
    Bounds makeCube(const Bounds& b) const;

    // These are aggregated as the Builder runs.
    Files& mutableFiles() { return *m_files; }

    std::unique_ptr<Schema> m_outSchema;
    std::unique_ptr<Schema> m_schema;

    std::unique_ptr<Bounds> m_boundsConforming;
    std::unique_ptr<Bounds> m_boundsCubic;

    std::unique_ptr<Files> m_files;
    std::unique_ptr<DataIo> m_dataIo;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Version> m_version;
    std::unique_ptr<Srs> m_srs;
    std::unique_ptr<Subset> m_subset;

    const bool m_trustHeaders = true;

    const uint64_t m_ticks;
    const uint64_t m_startDepth;
    const uint64_t m_sharedDepth;

    const uint64_t m_overflowDepth;
    const uint64_t m_overflowThreshold;

    uint64_t m_hierarchyStep;

    bool m_merged = false;
};

} // namespace entwine

