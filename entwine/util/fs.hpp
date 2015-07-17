/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>

namespace entwine
{
namespace fs
{

bool mkdirp(std::string dir);
bool removeFile(std::string filename);

} // namespace fs
} // namespace entwine

