/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/chunk-reader.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

ChunkReader::ChunkReader(
        const Schema& schema,
        const Id& id,
        const std::size_t maxPoints)
    : m_schema(schema)
    , m_id(id)
    , m_maxPoints(maxPoints)
{ }

std::unique_ptr<ChunkReader> ChunkReader::create(
        const Schema& schema,
        const Id& id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> data)
{
    std::unique_ptr<ChunkReader> reader;

    const ChunkType type(Chunk::getType(*data));

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
        const Id& id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> data)
    : ChunkReader(schema, id, maxPoints)
    , m_data()
{
    // TODO Direct copy/paste from SparseChunkData ctor.
    const std::size_t numPoints(SparseChunkData::popNumPoints(*data));

    const Schema sparse(SparseChunkData::makeSparse(m_schema));
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

const char* SparseReader::getData(const Id& rawIndex) const
{
    const char* pos(0);

    auto it(m_data.find(rawIndex));

    if (it != m_data.end())
    {
        pos = it->second.data();
    }

    return pos;
}

ContiguousReader::ContiguousReader(
        const Schema& schema,
        const Id& id,
        const std::size_t maxPoints,
        std::unique_ptr<std::vector<char>> compressed)
    : ChunkReader(schema, id, maxPoints)
    , m_data(
            Compression::decompress(
                *compressed,
                m_schema,
                m_maxPoints * m_schema.pointSize()))
{ }

const char* ContiguousReader::getData(const Id& rawIndex) const
{
    const std::size_t normal((rawIndex - m_id).getSimple());

    if (normal >= m_maxPoints)
    {
        throw std::runtime_error("Invalid index to getData");
    }

    return m_data->data() + normal * m_schema.pointSize();
}

std::unique_ptr<ChunkIter> ChunkIter::create(const ChunkReader& chunkReader)
{
    if (chunkReader.sparse())
        return std::unique_ptr<ChunkIter>(
                new SparseIter(
                    static_cast<const SparseReader&>(chunkReader)));
    else
        return std::unique_ptr<ChunkIter>(
                new ContiguousIter(
                    static_cast<const ContiguousReader&>(chunkReader)));
}

ContiguousIter::ContiguousIter(const ContiguousReader& reader)
    : m_index(0)
    , m_maxPoints(reader.m_maxPoints)
    , m_data(*reader.m_data)
    , m_pointSize(reader.m_schema.pointSize())
{ }

} // namespace entwine

