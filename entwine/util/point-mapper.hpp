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

#include <cstddef>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <entwine/tree/roller.hpp>
#include <entwine/types/elastic-atomic.hpp>
#include <entwine/util/file-descriptor.hpp>

namespace entwine
{

class Branch;
class Clipper;
class PointInfo;
class Pool;
class S3;

namespace fs
{

class Slot
{
public:
    Slot(
            const Schema& schema,
            const FileDescriptor& fd,
            std::size_t firstPoint);
    ~Slot();

    bool addPoint(
            PointInfo** toAddPtr,
            const Roller& roller,
            std::size_t index);

    bool hasPoint(std::size_t index);
    Point getPoint(std::size_t index);
    std::vector<char> getPointData(std::size_t index);

private:
    const Schema& m_schema;
    char* m_mapping;
    std::vector<char> m_data;
    std::vector<ElasticAtomic<const Point*>> m_points;
    std::vector<std::mutex> m_locks;
};

class PointMapper
{
public:
    PointMapper(
            const Schema& schema,
            const std::string& filename,
            std::size_t fileSize,
            std::size_t firstPoint,
            std::size_t numPoints);
    ~PointMapper();

    bool addPoint(PointInfo** toAddPtr, const Roller& roller);
    bool hasPoint(std::size_t index);
    Point getPoint(std::size_t index);
    std::vector<char> getPointData(std::size_t index);

    void grow(Clipper* clipper, std::size_t index);
    void clip(Clipper* clipper, std::size_t index);

    // Not safe to call during modification - assumes a static state.
    std::vector<std::size_t> ids() const;

    void finalize(
            S3& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            const std::size_t start,
            const std::size_t chunkSize);

private:
    const Schema& m_schema;
    FileDescriptor m_fd;

    const std::size_t m_fileSize;
    const std::size_t m_firstPoint;

    std::vector<ElasticAtomic<Slot*>> m_slots;
    std::vector<std::set<const Clipper*>> m_refs;
    std::vector<std::set<std::size_t>> m_ids;
    std::vector<std::mutex> m_locks;
};

} // namespace fs
} // namespace entwine

