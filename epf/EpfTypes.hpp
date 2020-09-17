#pragma once

#include <pdal/Dimension.hpp>
#include <pdal/util/Bounds.hpp>

#include <cstdint>
#include <memory>
#include <vector>

namespace epf
{

using DataVec = std::vector<uint8_t>;
using DataVecPtr = std::unique_ptr<DataVec>;

struct Error : public std::runtime_error
{
    Error(const std::string& err) : std::runtime_error(err)
    {}
};

struct FileDimInfo
{
    FileDimInfo(const std::string& name) : name(name)
    {}

    std::string name;
    pdal::Dimension::Id dim;
    int offset;
    pdal::Dimension::Type type;
};

struct FileInfo
{
    std::string filename;
    std::string driver;
    std::vector<FileDimInfo> dimInfo;
    pdal::BOX3D bounds;
};

} // namespace
