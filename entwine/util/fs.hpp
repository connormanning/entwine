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

class Fs
{
public:
    // Returns true if the directory did not exist before and was created.
    static bool mkdir(const std::string& path);

    // Returns true if the directory was created or already existed.
    static bool mkdirp(const std::string& path);

    // Returns true if file exists (can be opened for reading).
    static bool fileExists(const std::string& path);
};

} // namespace entwine

