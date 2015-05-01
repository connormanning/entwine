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

#include <fstream>
#include <ios>
#include <string>
#include <vector>

namespace entwine
{
namespace fs
{

// Binary output, overwriting any existing file with a conflicting name.
const std::ios_base::openmode binaryTruncMode(
        std::ofstream::binary |
        std::ofstream::out |
        std::ofstream::trunc);

// Returns true if the directory did not exist before and was created.
bool mkdir(const std::string& dir);

// Returns true if the directory was created or already existed.
bool mkdirp(const std::string& dir);

// Returns true if file exists (can be opened for reading).
bool fileExists(const std::string& filename);

// Returns true if file successfully removed.
bool removeFile(const std::string& filename);

std::string readFile(const std::string& filename);
std::vector<char> readBinaryFile(const std::string& filename);

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

} // namespace fs
} // namespace entwine

