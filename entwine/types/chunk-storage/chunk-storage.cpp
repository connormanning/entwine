/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/chunk-storage/chunk-storage.hpp>

#include <entwine/types/chunk-storage/binary.hpp>
#include <entwine/types/chunk-storage/lazperf.hpp>
#include <entwine/types/chunk-storage/laszip.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<ChunkStorage> ChunkStorage::create(
        const Metadata& m,
        const Config& c)
{
    const std::string s(c.dataStorage());
    if (s == "laszip")  return makeUnique<LasZipStorage>(m);
    /*
    if (s == "lazperf") return makeUnique<LazPerfStorage>(m);
    if (s == "binary")  return makeUnique<BinaryStorage>(m);
    */
    throw std::runtime_error("Invalid chunk compression type");
}

} // namespace entwine

