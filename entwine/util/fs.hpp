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

#include <ios>
#include <string>
#include <vector>

namespace entwine
{

// Pretty much everything here is currently UNIX-only.
namespace fs
{
    // Returns true if the directory did not exist before and was created.
    bool mkdir(const std::string& dir);

    // Returns true if the directory was created or already existed.
    bool mkdirp(const std::string& dir);

    // Returns true if file exists (can be opened for reading).
    bool fileExists(const std::string& filename);

    // Returns true if file successfully removed.
    bool removeFile(const std::string& filename);

    // Returns true if successfully written.
    bool writeFile(
            const std::string& filename,
            const std::vector<char>& contents,
            std::ios_base::openmode mode = std::ios_base::out);

    bool writeFile(
            const std::string& filename,
            const std::string& contents,
            std::ios_base::openmode mode = std::ios_base::out);

    bool writeFile(
            const std::string& filename,
            const char* data,
            std::size_t size,
            std::ios_base::openmode mode = std::ios_base::out);

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

