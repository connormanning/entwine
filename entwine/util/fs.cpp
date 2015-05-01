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
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <iostream>

namespace entwine
{
namespace fs
{

bool mkdir(const std::string& dir)
{
    const bool err(::mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return !err;
}

bool mkdirp(const std::string& dir)
{
    const bool err(::mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return (!err || errno == EEXIST);
}

bool fileExists(const std::string& filename)
{
    std::ifstream stream(filename);
    return stream.good();
}

bool removeFile(const std::string& filename)
{
    return remove(filename.c_str()) == 0;
}

std::string readFile(const std::string& filename)
{
    auto raw(readBinaryFile(filename));
    return std::string(raw.begin(), raw.end());
}

std::vector<char> readBinaryFile(const std::string& filename)
{
    std::ifstream stream(filename, std::ios::in | std::ios::binary);

    if (!stream.good())
    {
        throw std::runtime_error("Could not read file " + filename);
    }

    stream.seekg(0, std::ios::end);
    std::vector<char> data(static_cast<std::size_t>(stream.tellg()));
    stream.seekg(0, std::ios::beg);
    stream.read(data.data(), data.size());
    stream.close();

    return data;
}

bool writeFile(
        const std::string& filename,
        const std::vector<char>& contents,
        const std::ios_base::openmode mode)
{
    return writeFile(filename, contents.data(), contents.size(), mode);
}

bool writeFile(
        const std::string& filename,
        const std::string& contents,
        const std::ios_base::openmode mode)
{
    return writeFile(filename, contents.data(), contents.size(), mode);
}

bool writeFile(
        const std::string& filename,
        const char* data,
        const std::size_t size,
        const std::ios_base::openmode mode)
{
    std::ofstream writer(filename, mode);
    if (!writer.good()) return false;
    writer.write(data, size);
    return writer.good();
}

} // namespace fs
} // namespace entwine

