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

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Endpoint; }

class BBox;
class Manifest;
class Reprojection;
class Schema;
class Structure;
class Subset;

class Metadata
{
    friend class Builder;

public:
    Metadata(
            const BBox& bboxConforming,
            const Schema& schema,
            const Structure& structure,
            const Manifest& manifest,
            const Reprojection* reprojection = nullptr,
            const Subset* subset = nullptr,
            bool trustHeaders = true,
            bool compress = true);

    Metadata(
            const arbiter::Endpoint& endpoint,
            const std::size_t* subsetId = nullptr);

    Metadata(const Metadata& other);

    void merge(const Metadata& other);

    void save(const arbiter::Endpoint& endpoint) const;

    const BBox& bboxConforming() const;
    const BBox& bboxEpsilon() const;
    const BBox& bbox() const;
    const BBox* bboxSubset() const;
    const Schema& schema() const;
    const Structure& structure() const;
    const Manifest& manifest() const;
    const Reprojection* reprojection() const { return m_reprojection.get(); }
    const Subset* subset() const { return m_subset.get(); }

    const std::string& srs() const { return m_srs; }
    const std::vector<std::string>& errors() const { return m_errors; }

    bool trustHeaders() const { return m_trustHeaders; }
    bool compress() const { return m_compress; }

    std::string postfix(bool isColdChunk = false) const;
    void makeWhole();

private:
    // These are aggregated as the Builder runs.
    Manifest& manifest();
    std::string& srs() { return m_srs; }
    std::vector<std::string>& errors() { return m_errors; }

    std::unique_ptr<BBox> m_bboxConforming;
    std::unique_ptr<BBox> m_bboxEpsilon;
    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Subset> m_subset;

    std::string m_srs;
    bool m_trustHeaders;
    bool m_compress;

    std::vector<std::string> m_errors;
};

} // namespace entwine

