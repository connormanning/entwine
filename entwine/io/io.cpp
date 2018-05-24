/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <entwine/io/laz.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<DataIo> DataIo::create(const Metadata& m, std::string type)
{
    if (type == "laz") return makeUnique<Laz>(m);
}

} // namespace entwine

