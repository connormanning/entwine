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
    writer.write(data, size);
    return writer.good();
}

FileDescriptor::FileDescriptor(const std::string& filename, const int flags)
    : m_fd(open(filename, flags))
{ }

FileDescriptor::~FileDescriptor()
{
    if (m_fd != -1)
    {
        if (close(m_fd) < 0)
        {
            std::cout << "Error closing file descriptor!" << std::endl;
        }
    }
}

bool FileDescriptor::good() const
{
    return m_fd != -1;
}

int FileDescriptor::id() const
{
    return m_fd;
}

int FileDescriptor::open(const std::string& filename, int flags)
{
    return open(filename.c_str(), flags);
}

} // namespace fs
} // namespace entwine

