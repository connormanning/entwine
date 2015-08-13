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
    , m_raw()
    , m_ids()
{
    // TODO Direct copy/paste from SparseChunkData ctor.
    const std::size_t numPoints(SparseChunkData::popNumPoints(*data));
    const std::size_t nativePointSize(m_schema.pointSize());

    m_raw.resize(nativePointSize * numPoints);

    const Schema sparse(SparseChunkData::makeSparse(m_schema));
    const std::size_t sparsePointSize(sparse.pointSize());

    auto squashed(
            Compression::decompress(
                *data,
                sparse,
                numPoints * sparsePointSize));

    const char* in(squashed->data());
    const char* end(squashed->data() + squashed->size());

    uint64_t key(0);
    char* out(m_raw.data());

    const std::size_t keySize(sizeof(uint64_t));

    while (in != end)
    {
        std::memcpy(&key, in, keySize);
        std::memcpy(out, in + keySize, nativePointSize);

        m_ids[key] = out;

        in += sparsePointSize;
        out += nativePointSize;
    }
}

const char* SparseReader::getData(const Id& rawIndex) const
{
    const char* pos(0);

    auto it(m_ids.find(normalize(rawIndex)));

    if (it != m_ids.end())
    {
        pos = it->second;
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
    const std::size_t normal(normalize(rawIndex));

    if (normal >= m_maxPoints)
    {
        throw std::runtime_error("Invalid index to getData");
    }

    return m_data->data() + normal * m_schema.pointSize();
}

ChunkIter::ChunkIter(const ChunkReader& reader)
    : m_data(reader.getRaw())
    , m_index(0)
    , m_numPoints(reader.numPoints())
    , m_pointSize(reader.m_schema.pointSize())
{ }

} // namespace entwine

