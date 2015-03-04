/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "sleepy-tree.hpp"

#include <limits>
#include <cmath>
#include <memory>
#include <string>
#include <thread>

#include <pdal/Charbuf.hpp>
#include <pdal/Compression.hpp>
#include <pdal/Dimension.hpp>
#include <pdal/PointBuffer.hpp>
#include <pdal/Utils.hpp>

#include "compression/stream.hpp"
#include "compression/util.hpp"
#include "http/collector.hpp"
#include "tree/roller.hpp"
#include "types/bbox.hpp"
#include "types/schema.hpp"

SleepyTree::SleepyTree(
        const std::string& dir,
        const BBox& bbox,
        const Schema& schema,
        const std::size_t baseDepth,
        const std::size_t flatDepth,
        const std::size_t diskDepth)
    : m_dir(dir)
    , m_bbox(new BBox(bbox))
    , m_schema(new Schema(schema))
    , m_numPoints(0)
    , m_registry(
            new Registry(
                m_schema->stride(),
                baseDepth,
                flatDepth,
                diskDepth))
{ }

SleepyTree::SleepyTree(const std::string& dir)
    : m_dir(dir)
    , m_bbox()
    , m_schema()
    , m_numPoints(0)
    , m_registry()
{
    load();
}

SleepyTree::~SleepyTree()
{ }

void SleepyTree::insert(const pdal::PointBuffer* pointBuffer, Origin origin)
{
    Point point;

    for (std::size_t i = 0; i < pointBuffer->size(); ++i)
    {
        point.x = pointBuffer->getFieldAs<double>(pdal::Dimension::Id::X, i);
        point.y = pointBuffer->getFieldAs<double>(pdal::Dimension::Id::Y, i);

        if (m_bbox->contains(point))
        {
            Roller roller(*m_bbox.get());

            PointInfo* pointInfo(
                    new PointInfo(
                        m_schema->pointContext(),
                        pointBuffer,
                        i,
                        origin));

            m_registry->put(&pointInfo, roller);
            ++m_numPoints;
        }
    }
}

void SleepyTree::save()
{
    {
        std::ofstream metaStream(
                metaPath(),
                std::ofstream::out | std::ofstream::trunc);

        const std::string metaString(meta().toStyledString());
        metaStream.write(metaString.data(), metaString.size());
    }

    // TODO Write others besides baseData.  Probably need to call something
    // like Registry::write().
    const std::string dataPath(m_dir + "/0");
    std::ofstream dataStream(
            dataPath,
            std::ofstream::out | std::ofstream::trunc | std::ofstream::binary);

    const uint64_t uncompressedSize(m_registry->baseData().size());

    std::unique_ptr<std::vector<char>> compressed(
            Compression::compress(
                m_registry->baseData(),
                m_schema->pointContext().dimTypes()));

    const uint64_t compressedSize(compressed->size());

    dataStream.write(
            reinterpret_cast<const char*>(&uncompressedSize),
            sizeof(uint64_t));
    dataStream.write(
            reinterpret_cast<const char*>(&compressedSize),
            sizeof(uint64_t));
    dataStream.write(compressed->data(), compressed->size());
    dataStream.close();
}

void SleepyTree::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        std::ifstream metaStream(metaPath());

        reader.parse(metaStream, meta, false);
    }

    m_bbox.reset(new BBox(BBox::fromJson(meta["bbox"])));
    m_schema.reset(new Schema(Schema::fromJson(meta["schema"])));

    // TODO Read others besides baseData.
    const std::string dataPath(m_dir + "/0");

    std::ifstream dataStream(
            dataPath,
            std::ifstream::in | std::ifstream::binary);
    if (!dataStream.good())
    {
        throw std::runtime_error("Could not open " + dataPath);
    }

    uint64_t uncSize(0), cmpSize(0);
    dataStream.read(reinterpret_cast<char*>(&uncSize), sizeof(uint64_t));
    dataStream.read(reinterpret_cast<char*>(&cmpSize), sizeof(uint64_t));

    std::vector<char> compressed(cmpSize);
    dataStream.read(compressed.data(), compressed.size());

    std::unique_ptr<std::vector<char>> uncompressed(
            Compression::decompress(
                compressed,
                m_schema->pointContext().dimTypes(),
                uncSize));

    const Json::Value& treeMeta(meta["tree"]);
    m_registry.reset(
            new Registry(
                m_schema->stride(),
                uncompressed.release(),
                treeMeta["baseDepth"].asUInt64(),
                treeMeta["flatDepth"].asUInt64(),
                treeMeta["diskDepth"].asUInt64()));
}

const BBox& SleepyTree::getBounds() const
{
    return *m_bbox.get();
}

MultiResults SleepyTree::getPoints(
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    Roller roller(*m_bbox.get());
    MultiResults results;
    m_registry->getPoints(
            roller,
            results,
            depthBegin,
            depthEnd);

    return results;
}

MultiResults SleepyTree::getPoints(
        const BBox& bbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    Roller roller(*m_bbox.get());
    MultiResults results;
    m_registry->getPoints(
            roller,
            results,
            bbox,
            depthBegin,
            depthEnd);

    return results;
}

pdal::PointContext SleepyTree::pointContext() const
{
    return m_schema->pointContext();
}

std::vector<char>& SleepyTree::data(uint64_t id)
{
    // TODO Other IDs besides base.  Let the Registry decide based on ID.
    return m_registry->baseData();
}

std::size_t SleepyTree::numPoints() const
{
    return m_numPoints;
}

Json::Value SleepyTree::meta() const
{
    Json::Value tree;
    tree["baseDepth"] = static_cast<Json::UInt64>(m_registry->baseDepth());
    tree["flatDepth"] = static_cast<Json::UInt64>(m_registry->flatDepth());
    tree["diskDepth"] = static_cast<Json::UInt64>(m_registry->diskDepth());

    Json::Value meta;
    meta["bbox"] =   m_bbox->toJson();
    meta["schema"] = m_schema->toJson();
    meta["tree"] =   tree;

    return meta;
}

std::string SleepyTree::metaPath() const
{
    return m_dir + "/meta";
}

