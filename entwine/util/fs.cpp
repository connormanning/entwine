/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/fs.hpp>

#include <sys/stat.h>

#include <fstream>

namespace entwine
{

bool Fs::mkdir(const std::string& path)
{
    const bool err(::mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return !err;
}

bool Fs::mkdirp(const std::string& path)
{
    const bool err(::mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return (!err || errno == EEXIST);
}

bool Fs::fileExists(const std::string& path)
{
    std::ifstream stream(path);
    return stream.good();
}

} // namespace entwine

