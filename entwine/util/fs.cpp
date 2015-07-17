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

#include <errno.h>
#include <glob.h>
#include <sys/stat.h>
#include <unistd.h>

namespace entwine
{

namespace fs
{

bool mkdirp(std::string dir)
{
    const bool err(::mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return (!err || errno == EEXIST);
}

bool removeFile(std::string filename)
{
    return remove(filename.c_str()) == 0;
}

} // namespace fs
} // namespace entwine

