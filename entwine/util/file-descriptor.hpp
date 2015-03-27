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

#include <fcntl.h>

#include <string>

namespace entwine
{
namespace fs
{

class FileDescriptor
{
public:
    FileDescriptor(const std::string& filename, int flags = O_RDWR);
    ~FileDescriptor();

    bool good() const;
    int id() const;

private:
    const int m_fd;
};

} // namespace fs
} // namespace entwine

