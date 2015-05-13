/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/chunk.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/drivers/source.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

namespace
{
    double getThreshold(const Schema& schema)
    {
        const std::size_t pointSize(schema.pointSize());

        return pointSize / (pointSize + sizeof(std::size_t));
    }

    /*
    DimList makeSparse(const Schema& schema)
    {
        DimList dims(1, DimInfo("EntryId", "unsigned", 8));
        dims.insert(dims.end(), schema.dims().begin(), schema.dims().end());
        return dims;
    }
    */
}

Entry::Entry(char* data)
    : m_point(0)
    , m_mutex()
    , m_data(data)
{ }

Entry::Entry(const Point* point, char* data)
    : m_point(point)
    , m_mutex()
    , m_data(data)
{ }

Entry::~Entry()
{
    if (m_point.atom.load()) delete m_point.atom.load();
}

ChunkData::ChunkData(
        const Schema& schema,
        const std::size_t id,
        std::size_t maxPoints)
    : m_schema(schema)
    , m_id(id)
    , m_maxPoints(maxPoints)
{ }

ChunkData::ChunkData(const ChunkData& other)
    : m_schema(other.m_schema)
    , m_id(other.m_id)
    , m_maxPoints(other.m_maxPoints)
{ }

ChunkData::~ChunkData()
{ }

std::size_t ChunkData::normalize(const std::size_t rawIndex)
{
    assert(rawIndex >= m_id);
    assert(rawIndex < m_id + m_maxPoints);

    return rawIndex - m_id;
}

std::size_t ChunkData::endId() const
{
    return m_id + m_maxPoints;
}
















/*
SparseChunkData::SparseChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints)
    : ChunkData(schema, id, maxPoints)
    , m_entries()
    , m_mutex()
{ }

SparseChunkData::SparseChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char>& compressedData)
    : ChunkData(schema, id, maxPoints)
    , m_entries()
    , m_mutex()
{
    uint64_t numPoints(0);
    const std::size_t numPointsOffset(compressedData.size() - sizeof(uint64_t));

    std::memcpy(
            &numPoints,
            compressedData.data() + numPointsOffset,
            sizeof(uint64_t));

    compressedData.resize(numPointsOffset);

    const Schema sparse(makeSparse(m_schema));

    const std::size_t sparsePointSize(sparse.pointSize());
    const std::size_t nativePointSize(m_schema.pointSize());

    auto squashed(
            Compression::decompress(
                compressedData,
                sparse,
                numPoints * sparsePointSize));

    uint64_t currentId(0);

    for (
            std::size_t offset(0);
            offset < squashed->size();
            offset += sparsePointSize)
    {
        char* pos(squashed->data() + offset);
        std::memcpy(&currentId, pos, sizeof(uint64_t));

        SparseEntry val(nativePointSize, m_schema, pos + sizeof(uint64_t));

        m_entries.insert({ currentId, val });
    }
}

Entry& SparseChunkData::getEntry(const std::size_t rawIndex)
{
    // TODO
    lock!
    m_entries.insert({ rawIndex, SparseEntry(m_schema.pointSize()) });

    SparseEntry& sparse(m_entries.at(rawIndex));

    return std::unique_ptr<Entry>(
            new Entry(
                sparse.point(),
                sparse.data(),
                m_mutex));
}

void SparseChunkData::save(Source& source)
{
    write(source, m_id, m_id + m_maxPoints);
}

void SparseChunkData::finalize(
        Source& source,
        std::vector<std::size_t>& ids,
        std::mutex& idsMutex,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    // TODO Duplicate code with Contiguous.

    // This may only occur for the base branch's chunk, since the start of
    // chunked data must occur within or at the end of the base branch.
    if (start > m_id)
    {
        write(source, m_id, start);

        std::lock_guard<std::mutex> lock(idsMutex);
        ids.push_back(m_id);
    }

    for (std::size_t id(std::max(start, m_id)); id < endId(); id += chunkPoints)
    {
        write(source, id, id + chunkPoints);

        std::lock_guard<std::mutex> lock(idsMutex);
        ids.push_back(id);
    }
}

void SparseChunkData::write(
        Source& source,
        const std::size_t begin,
        const std::size_t end)
{
    Schema sparse(makeSparse(m_schema));

    std::vector<char> data(squash(sparse, begin, end));

    auto compressed(Compression::compress(data.data(), data.size(), sparse));

    const uint64_t numPoints(data.size() / sparse.pointSize());

    compressed->insert(
            compressed->end(),
            &numPoints,
            &numPoints + sizeof(uint64_t));

    compressed->push_back(Sparse);

    source.put(std::to_string(begin), *compressed);
}

std::vector<char> SparseChunkData::squash(
        const Schema& sparse,
        const std::size_t begin,
        const std::size_t end)
{
    std::vector<char> squashed;

    const std::size_t nativePointSize(m_schema.pointSize());
    const std::size_t sparsePointSize(sparse.pointSize());

    const auto beginIt(m_entries.lower_bound(begin));
    const auto endIt(m_entries.upper_bound(end - 1));

    char* pos(0);

    for (auto it(beginIt); it != endIt; ++it)
    {
        const std::size_t startSize(squashed.size());
        squashed.resize(startSize + sparsePointSize);

        pos = squashed.data() + startSize;

        const std::size_t id(it->first);
        const char* data(it->second.data());

        std::memcpy(pos, &id, sizeof(uint64_t));
        pos += sizeof(uint64_t);

        std::memcpy(pos, data, nativePointSize);
        pos += nativePointSize;
    }

    return squashed;
}

*/







ContiguousChunkData::ContiguousChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints)
    : ChunkData(schema, id, maxPoints)
    , m_entries(m_maxPoints)
    , m_data()
{
    makeEmpty();
}

ContiguousChunkData::ContiguousChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char>& compressedData)
    : ChunkData(schema, id, maxPoints)
    , m_entries(m_maxPoints)
    , m_data()
{
    m_data =
        Compression::decompress(
                compressedData,
                m_schema,
                m_maxPoints * m_schema.pointSize());

    double x(0);
    double y(0);

    const std::size_t pointSize(m_schema.pointSize());

    for (std::size_t i(0); i < maxPoints; ++i)
    {
        char* pos(m_data->data() + i * pointSize);

        SinglePointTable table(m_schema, pos);
        LinkingPointView view(table);

        x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
        y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

        m_entries[i].reset(
                new Entry(
                    Point::exists(x, y) ? new Point(x, y) : 0,
                    m_data->data() + pointSize * i));
    }
}

/*
ContiguousChunkData::ContiguousChunkData(const SparseChunkData& other)
    : ChunkData(other)
    , m_entries()
    , m_data()
{
    makeEmpty();

    std::lock_guard<std::mutex> lock(other.mutex());

    for (std::size_t id : other.entries())
    {
        // TODO
    }
}
*/

Entry& ContiguousChunkData::getEntry(std::size_t rawIndex)
{
    const std::size_t index(normalize(rawIndex));
    auto& entry(m_entries[index]);

    /*
    if (!entry)
    {
        entry.reset(new Entry(m_data->data() + index * m_schema.pointSize()));
    }
    */

    return *entry;
}

void ContiguousChunkData::save(Source& source)
{
    write(source, m_id, endId());
}

void ContiguousChunkData::finalize(
        Source& source,
        std::vector<std::size_t>& ids,
        std::mutex& idsMutex,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    // This may only occur for the base branch's chunk, since the start of
    // chunked data must occur within or at the end of the base branch.
    if (start > m_id)
    {
        write(source, m_id, start);

        std::lock_guard<std::mutex> lock(idsMutex);
        ids.push_back(m_id);
    }

    for (std::size_t id(std::max(start, m_id)); id < endId(); id += chunkPoints)
    {
        write(source, id, id + chunkPoints);

        std::lock_guard<std::mutex> lock(idsMutex);
        ids.push_back(id);
    }
}

void ContiguousChunkData::write(
        Source& source,
        const std::size_t begin,
        const std::size_t end)
{
    const std::size_t normalized(normalize(begin));
    const std::size_t pointSize(m_schema.pointSize());

    auto compressed(
            Compression::compress(
                m_data->data() + normalized * pointSize,
                (end - begin) * pointSize,
                m_schema));

    compressed->push_back(Contiguous);

    source.put(std::to_string(begin), *compressed);
}

void ContiguousChunkData::makeEmpty()
{
    std::unique_ptr<SimplePointTable> table(new SimplePointTable(m_schema));
    pdal::PointView view(*table);

    const auto emptyCoord(Point::emptyCoord());
    const std::size_t pointSize(m_schema.pointSize());

    for (std::size_t i(0); i < m_maxPoints; ++i)
    {
        view.setField(pdal::Dimension::Id::X, i, emptyCoord);
        view.setField(pdal::Dimension::Id::Y, i, emptyCoord);
    }

    m_data.reset(new std::vector<char>(table->data()));

    for (std::size_t i(0); i < m_maxPoints; ++i)
    {
        m_entries[i].reset(new Entry(m_data->data() + pointSize * i));
    }
}














std::unique_ptr<ChunkData> ChunkDataFactory::create(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char>& data)
{
    std::unique_ptr<ChunkData> chunk;

    if (!data.empty())
    {
        const char type(data.back());
        data.pop_back();

        if (type == Sparse)
        {
            // chunk.reset(new SparseChunkData(schema, id, maxPoints, data));
        }
        else if (type == Contiguous)
        {
            chunk.reset(new ContiguousChunkData(schema, id, maxPoints, data));
        }
    }

    if (!chunk)
    {
        throw std::runtime_error("Invalid serialized chunk");
    }

    return chunk;
}

















Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints)
    : m_chunkData(new ContiguousChunkData(schema, id, maxPoints))
            // new SparseChunkData(schema, id, maxPoints)) TODO
    , m_threshold(getThreshold(schema))
{ }

Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char> data)
    : m_chunkData(ChunkDataFactory::create(schema, id, maxPoints, data))
    , m_threshold(getThreshold(schema))
{ }

Entry& Chunk::getEntry(std::size_t rawIndex)
{
    // TODO
    /*
    if (m_chunkData->isSparse())
    {
        const double ratio(
                static_cast<double>(m_chunkData->numPoints()) /
                static_cast<double>(m_chunkData->maxPoints()));

        if (ratio > m_threshold)
        {
            SparseChunkData& sparse(
                    reinterpret_cast<SparseChunkData&>(*m_chunkData));

            m_chunkData.reset(new ContiguousChunkData(sparse));
        }
    }
    */

    return m_chunkData->getEntry(rawIndex);
}

void Chunk::save(Source& source)
{
    m_chunkData->save(source);
}

void Chunk::finalize(
        Source& source,
        std::vector<std::size_t>& ids,
        std::mutex& idsMutex,
        const std::size_t start,
        const std::size_t chunkPoints)
{
    m_chunkData->finalize(source, ids, idsMutex, start, chunkPoints);
}

} // namespace entwine

