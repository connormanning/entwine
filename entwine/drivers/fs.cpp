/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/drivers/fs.hpp>

#include <stdexcept>

#include <entwine/util/fs.hpp>

namespace entwine
{

std::vector<char> FsDriver::get(const std::string path)
{
    return fs::readBinaryFile(path);
}

void FsDriver::put(const std::string path, const std::vector<char>& data)
{
    if (!fs::writeFile(path, data, fs::binaryTruncMode))
    {
        throw std::runtime_error("Could not write " + path);
    }
}

} // namespace entwine

