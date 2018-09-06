/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/io.hpp>

#include <stdexcept>

#include <entwine/io/binary.hpp>
#include <entwine/io/laszip.hpp>
#include <entwine/io/zstandard.hpp>

#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<DataIo> DataIo::create(const Metadata& m, std::string type)
{
    if (type == "laszip") return makeUnique<Laz>(m);
    /*
    if (type == "binary") return makeUnique<Binary>(m);
    if (type == "zstandard") return makeUnique<Zstandard>(m);
    */
    throw std::runtime_error("Invalid data IO type: " + type);
}

} // namespace entwine

