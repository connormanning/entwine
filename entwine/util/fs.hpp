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

#include <string>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/defs.hpp>

namespace entwine
{

bool isDirectory(std::string path);
std::string getStem(std::string path);

// Accepts an array of inputs which are some combination of file/directory
// paths.  Input paths which are directories are globbed into their constituent
// files.
StringList resolve(
    const StringList& input,
    const arbiter::Arbiter& a = arbiter::Arbiter());

} // namespace entwine
