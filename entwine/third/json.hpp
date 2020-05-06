/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <entwine/third/mjson/json.hpp>

namespace entwine { using json = nlohmann::json; namespace nsjson = nlohmann; }

// Work around:
// https://github.com/nlohmann/json/issues/709
// https://github.com/google/googletest/pull/1186
namespace nlohmann
{
inline void PrintTo(json const& json, std::ostream* os)
{
    *os << json.dump();
}
}

