/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/point-mapper.hpp>

#include <sys/mman.h>

#include <cassert>
#include <cstring>
#include <iostream>

#include <entwine/tree/point-info.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/platform.hpp>

namespace
{
    const std::size_t numSlots(256);
}

namespace entwine
{
namespace fs
{

PointMapper::PointMapper(
        const Schema& schema,
        const std::string& filename,
        const std::size_t fileSize,
        const std::size_t firstPoint)
    : m_schema(schema)
    , m_fd(filename)
    , m_slotSize(fileSize / numSlots)
    , m_firstPointIndex(firstPoint)
    , m_mappings(numSlots, {0})
    , m_locks(numSlots)
{
    if (!fs::fileExists(filename))
    {
        throw std::runtime_error("File does not exist");
    }

    if (
            m_slotSize * numSlots != fileSize ||
            m_slotSize % platform::pageSize())
    {
        throw std::runtime_error("Invalid arguments to PointMapper");
    }
}

PointMapper::~PointMapper()
{
    for (auto& m : m_mappings)
    {
        if (char* mapping = m.atom.load())
        {
            if (msync(mapping, m_slotSize, MS_ASYNC) == -1)
            {
                throw std::runtime_error("Couldn't sync mapping");
            }
        }
    }
}

bool PointMapper::addPoint(const Roller& roller, PointInfo** toAddPtr)
{
    bool done(false);

    const std::size_t index(roller.pos());
    assert(index >= m_firstPointIndex);

    const std::size_t slotIndex(getSlotIndex(index));

    char* pos(ensureMapping(slotIndex) + getSlotOffset(index));

    SinglePointTable table(m_schema, pos);
    LinkingPointView view(table);

    std::lock_guard<std::mutex> lock(m_locks[slotIndex]);

    Point curPoint(
            view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
            view.getFieldAs<double>(pdal::Dimension::Id::Y, 0));

    PointInfo* toAdd(*toAddPtr);

    if (Point::exists(curPoint.x, curPoint.y))
    {
        const Point mid(roller.bbox().mid());

        if (toAdd->point->sqDist(mid) < curPoint.sqDist(mid))
        {
            // Pull out the old stored value.
            PointInfo* old(
                    new PointInfo(
                        new Point(curPoint),
                        pos,
                        m_schema.pointSize()));

            toAdd->write(pos);
            delete toAdd->point;
            delete toAdd;

            *toAddPtr = old;
        }
    }
    else
    {
        // Empty point here - store incoming.
        toAdd->write(pos);

        delete toAdd->point;
        delete toAdd;
        done = true;
    }

    return done;
}

Point PointMapper::getPoint(const std::size_t index)
{
    const std::size_t slotIndex(getSlotIndex(index));

    char* pos(ensureMapping(slotIndex) + getSlotOffset(index));

    SinglePointTable table(m_schema, pos);
    LinkingPointView view(table);

    return Point(
            view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
            view.getFieldAs<double>(pdal::Dimension::Id::Y, 0));
}

std::vector<char> PointMapper::getPointData(std::size_t index)
{
    const std::size_t slotIndex(getSlotIndex(index));

    char* pos(ensureMapping(slotIndex) + getSlotOffset(index));

    return std::vector<char>(pos, pos + m_schema.pointSize());
}

std::size_t PointMapper::getSlotIndex(std::size_t index) const
{
    return getGlobalOffset(index) / m_slotSize;
}

std::size_t PointMapper::getSlotOffset(std::size_t index) const
{
    return getGlobalOffset(index) % m_slotSize;
}

std::size_t PointMapper::getGlobalOffset(std::size_t index) const
{
    assert(index >= m_firstPointIndex);

    return (index - m_firstPointIndex) * m_schema.pointSize();
}
char* PointMapper::ensureMapping(const std::size_t slotIndex)
{
    auto& thisMapping(m_mappings[slotIndex].atom);

    if (!thisMapping.load())
    {
        std::lock_guard<std::mutex> lock(m_locks[slotIndex]);
        if (!thisMapping.load())
        {
            char* mapping(
                    static_cast<char*>(mmap(
                        0,
                        m_slotSize,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED,
                        m_fd.id(),
                        slotIndex * m_slotSize)));

            if (mapping == MAP_FAILED)
            {
                std::cout << "Mapping failed: " << strerror(errno) << std::endl;
                throw std::runtime_error("Could not create mapping!");
            }

            thisMapping.store(mapping);
        }
    }

    return thisMapping.load();
}

} // namespace fs
} // namespace entwine

