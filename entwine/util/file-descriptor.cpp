/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/file-descriptor.hpp>

#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

namespace entwine
{
namespace fs
{

FileDescriptor::FileDescriptor(const std::string& filename, const int flags)
    : m_fd(open(filename.c_str(), flags))
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

} // namespace fs
} // namespace entwine

