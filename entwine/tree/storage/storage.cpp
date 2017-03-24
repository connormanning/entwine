/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/storage/storage.hpp>

#include <entwine/tree/storage/binary.hpp>
#include <entwine/tree/storage/lazperf.hpp>
#include <entwine/tree/storage/laszip.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<ChunkStorage> ChunkStorage::create(
        const Metadata& m,
        const ChunkCompression c,
        const Json::Value& j)
{
    switch (c)
    {
        case ChunkCompression::LazPerf: return makeUnique<LazPerfStorage>(m, j);
        case ChunkCompression::LasZip: return makeUnique<LasZipStorage>(m, j);
        case ChunkCompression::None: return makeUnique<BinaryStorage>(m, j);
        default: throw std::runtime_error("Invalid chunk compression type");
    }
}

} // namespace entwine

