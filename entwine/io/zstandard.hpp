/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/binary.hpp>

namespace entwine
{
namespace io
{
namespace zstandard
{

void write(
    const Metadata& Metadata,
    const Endpoints& endpoints,
    const std::string filename,
    BlockPointTable& table,
    const Bounds bounds);

void read(
    const Metadata& metadata,
    const Endpoints& endpoints,
    std::string filename,
    VectorPointTable& table);

} // namespace zstandard
} // namespace io
} // namespace entwine
