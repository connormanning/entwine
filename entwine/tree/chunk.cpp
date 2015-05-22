/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/chunk.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/drivers/source.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

namespace
{
    double getThreshold(const Schema& schema)
    {
        const std::size_t pointSize(schema.pointSize());

        return
            static_cast<double>(pointSize) /
            static_cast<double>(pointSize + sizeof(std::size_t));
    }

    DimList makeSparse(const Schema& schema)
    {
        DimList dims(1, DimInfo("EntryId", "unsigned", 8));
        dims.insert(dims.end(), schema.dims().begin(), schema.dims().end());
        return dims;
    }

    ChunkType getChunkType(std::vector<char>& data)
    {
        ChunkType chunkType;

        if (!data.empty())
        {
            const char marker(data.back());
            data.pop_back();

            if (marker == Sparse) chunkType = Sparse;
            else if (marker == Contiguous) chunkType = Contiguous;
            else throw std::runtime_error("Invalid chunk type detected");
        }
        else
        {
            throw std::runtime_error("Invalid chunk data detected");
        }

        return chunkType;
    }
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

std::size_t ChunkData::endId() const
{
    return m_id + m_maxPoints;
}











SparseChunkData::SparseEntry::SparseEntry(const Schema& schema)
    : data(schema.pointSize())
    , entry(new Entry(data.data()))
{ }

SparseChunkData::SparseEntry::SparseEntry(const Schema& schema, char* pos)
    : data(pos, pos + schema.pointSize())
    , entry()
{
    SinglePointTable table(schema, pos);
    LinkingPointView view(table);

    const Point* point(
            new Point(
                view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                view.getFieldAs<double>(pdal::Dimension::Id::Y, 0)));

    entry.reset(new Entry(point, data.data()));
}

SparseChunkData::SparseChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints)
    : ChunkData(schema, id, maxPoints)
    , m_mutex()
    , m_entries()
{ }

SparseChunkData::SparseChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char>& compressedData)
    : ChunkData(schema, id, maxPoints)
    , m_mutex()
    , m_entries()
{
    const std::size_t numPoints(popNumPoints(compressedData));

    const Schema sparse(makeSparse(m_schema));
    const std::size_t sparsePointSize(sparse.pointSize());

    auto squashed(
            Compression::decompress(
                compressedData,
                sparse,
                numPoints * sparsePointSize));

    uint64_t key(0);
    char* pos(0);

    for (
            std::size_t offset(0);
            offset < squashed->size();
            offset += sparsePointSize)
    {
        pos = squashed->data() + offset;
        std::memcpy(&key, pos, sizeof(uint64_t));

        m_entries.emplace(key, SparseEntry(schema, pos + sizeof(uint64_t)));
    }
}

Entry* SparseChunkData::getEntry(const std::size_t rawIndex)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it(m_entries.find(rawIndex));

    if (it == m_entries.end())
    {
        it = m_entries.emplace(rawIndex, SparseEntry(m_schema)).first;
    }

    return it->second.entry.get();
}

void SparseChunkData::save(Source& source)
{
    Schema sparse(makeSparse(m_schema));

    std::vector<char> data(squash(sparse));

    auto compressed(Compression::compress(data.data(), data.size(), sparse));

    pushNumPoints(*compressed, m_entries.size());
    compressed->push_back(Sparse);

    source.put(std::to_string(m_id), *compressed);
}

std::vector<char> SparseChunkData::squash(const Schema& sparse)
{
    std::vector<char> squashed;

    const std::size_t nativePointSize(m_schema.pointSize());
    const std::size_t sparsePointSize(sparse.pointSize());

    assert(nativePointSize + sizeof(uint64_t) == sparsePointSize);

    const auto beginIt(m_entries.begin());
    const auto endIt(m_entries.end());

    std::vector<char> addition(sparsePointSize);
    char* pos(addition.data());

    for (auto it(beginIt); it != endIt; ++it)
    {
        const std::size_t id(it->first);
        const char* data(it->second.data.data());

        std::memcpy(pos, &id, sizeof(uint64_t));
        std::memcpy(pos + sizeof(uint64_t), data, nativePointSize);

        squashed.insert(squashed.end(), pos, pos + sparsePointSize);
    }

    return squashed;
}

void SparseChunkData::pushNumPoints(
        std::vector<char>& data,
        const std::size_t numPoints) const
{
    std::size_t startSize(data.size());
    data.resize(startSize + sizeof(uint64_t));
    std::memcpy(data.data() + startSize, &numPoints, sizeof(uint64_t));
}

std::size_t SparseChunkData::popNumPoints(
        std::vector<char>& compressedData)
{
    if (compressedData.size() < sizeof(uint64_t))
    {
        throw std::runtime_error("Invalid serialized sparse chunk");
    }

    uint64_t numPoints(0);
    const std::size_t numPointsOffset(compressedData.size() - sizeof(uint64_t));

    std::memcpy(
            &numPoints,
            compressedData.data() + numPointsOffset,
            sizeof(uint64_t));

    compressedData.resize(numPointsOffset);

    return numPoints;
}







ContiguousChunkData::ContiguousChunkData(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        const std::vector<char>& empty)
    : ChunkData(schema, id, maxPoints)
    , m_entries(m_maxPoints)
    , m_data(new std::vector<char>(empty))
{
    emptyEntries();
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

ContiguousChunkData::ContiguousChunkData(
        SparseChunkData& sparse,
        const std::vector<char>& empty)
    : ChunkData(sparse)
    , m_entries(m_maxPoints)
    , m_data(new std::vector<char>(empty))
{
    emptyEntries();

    const std::size_t pointSize(m_schema.pointSize());

    std::lock_guard<std::mutex> lock(sparse.m_mutex);

    for (auto& p : sparse.m_entries)
    {
        const std::size_t id(normalize(p.first));
        auto& sparseEntry(p.second);
        auto& myEntry(m_entries[id]);

        char* pos(m_data->data() + id * pointSize);

        myEntry = std::move(sparseEntry.entry);

        std::lock_guard<std::mutex> dataLock(myEntry->mutex());
        myEntry->setData(pos);

        std::memcpy(pos, sparseEntry.data.data(), pointSize);
    }

    sparse.m_entries.clear();
}

Entry* ContiguousChunkData::getEntry(std::size_t rawIndex)
{
    return m_entries[normalize(rawIndex)].get();
}

void ContiguousChunkData::save(Source& source)
{
    const std::size_t pointSize(m_schema.pointSize());

    auto compressed(
            Compression::compress(
                m_data->data(),
                m_maxPoints * pointSize,
                m_schema));

    compressed->push_back(Contiguous);

    source.put(std::to_string(m_id), *compressed);
}

void ContiguousChunkData::emptyEntries()
{
    const std::size_t pointSize(m_schema.pointSize());

    for (std::size_t i(0); i < m_maxPoints; ++i)
    {
        m_entries[i].reset(new Entry(m_data->data() + pointSize * i));
    }
}

std::size_t ContiguousChunkData::normalize(const std::size_t rawIndex)
{
    assert(rawIndex >= m_id);
    assert(rawIndex < m_id + m_maxPoints);

    return rawIndex - m_id;
}













std::unique_ptr<ChunkData> ChunkDataFactory::create(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char>& data)
{
    std::unique_ptr<ChunkData> chunk;

    const ChunkType type(getChunkType(data));

    if (type == Sparse)
    {
        chunk.reset(new SparseChunkData(schema, id, maxPoints, data));
    }
    else if (type == Contiguous)
    {
        chunk.reset(new ContiguousChunkData(schema, id, maxPoints, data));
    }

    return chunk;
}

















Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        const bool forceContiguous,
        const std::vector<char>& empty)
    : m_chunkData(
            forceContiguous ?
                static_cast<ChunkData*>(
                    new ContiguousChunkData(schema, id, maxPoints, empty)) :
                static_cast<ChunkData*>(
                    new SparseChunkData(schema, id, maxPoints)))
    , m_threshold(getThreshold(schema))
    , m_mutex()
    , m_converting(false)
    , m_empty(empty)
{ }

Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::vector<char> data,
        const std::vector<char>& empty)
    : m_chunkData(ChunkDataFactory::create(schema, id, maxPoints, data))
    , m_threshold(getThreshold(schema))
    , m_mutex()
    , m_converting(false)
    , m_empty(empty)
{ }

Entry* Chunk::getEntry(std::size_t rawIndex)
{
    if (m_chunkData->isSparse())
    {
        const double ratio(
                static_cast<double>(m_chunkData->numPoints()) /
                static_cast<double>(m_chunkData->maxPoints()));

        if (ratio > m_threshold)
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            if (!m_converting.load() && m_chunkData->isSparse())
            {
                m_converting.store(true);

                m_chunkData.reset(
                        new ContiguousChunkData(
                            reinterpret_cast<SparseChunkData&>(*m_chunkData),
                            m_empty));

                m_converting.store(false);
            }
        }
    }

    while (m_converting.load())
        ;

    return m_chunkData->getEntry(rawIndex);
}

void Chunk::save(Source& source)
{
    m_chunkData->save(source);
}












// TODO The Readers and the ChunkData classes should probably derive from
// something common.

ChunkReader::ChunkReader(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints)
    : m_schema(schema)
    , m_id(id)
    , m_maxPoints(maxPoints)
{ }

std::unique_ptr<ChunkReader> ChunkReader::create(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> data)
{
    std::unique_ptr<ChunkReader> reader;

    const ChunkType type(getChunkType(*data));

    if (type == Sparse)
    {
        reader.reset(new SparseReader(schema, id, maxPoints, std::move(data)));
    }
    else if (type == Contiguous)
    {
        reader.reset(
                new ContiguousReader(schema, id, maxPoints, std::move(data)));
    }

    return reader;
}

SparseReader::SparseReader(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> data)
    : ChunkReader(schema, id, maxPoints)
    , m_data()
{
    // TODO Direct copy/paste from SparseChunkData ctor.
    const std::size_t numPoints(SparseChunkData::popNumPoints(*data));

    const Schema sparse(makeSparse(m_schema));
    const std::size_t sparsePointSize(sparse.pointSize());

    auto squashed(
            Compression::decompress(
                *data,
                sparse,
                numPoints * sparsePointSize));

    uint64_t key(0);
    char* pos(0);

    for (
            std::size_t offset(0);
            offset < squashed->size();
            offset += sparsePointSize)
    {
        pos = squashed->data() + offset;
        std::memcpy(&key, pos, sizeof(uint64_t));

        std::vector<char> point(pos + sizeof(uint64_t), pos + sparsePointSize);

        m_data.emplace(key, point);
    }
}

char* SparseReader::getData(const std::size_t rawIndex)
{
    char* pos(0);

    auto it(m_data.find(rawIndex));

    if (it != m_data.end())
    {
        pos = it->second.data();
    }

    return pos;
}













ContiguousReader::ContiguousReader(
        const Schema& schema,
        const std::size_t id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> compressed)
    : ChunkReader(schema, id, maxPoints)
    , m_data(
            Compression::decompress(
                *compressed,
                m_schema,
                m_maxPoints * m_schema.pointSize()))
{ }

char* ContiguousReader::getData(const std::size_t rawIndex)
{
    const std::size_t normal(rawIndex - m_id);

    return m_data->data() + normal * m_schema.pointSize();
}

} // namespace entwine

