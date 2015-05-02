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

#include <map>
#include <string>
#include <vector>

namespace entwine
{

class Driver
{
public:
    virtual ~Driver() { }

    virtual std::vector<char> get(std::string path) = 0;
    virtual void put(std::string path, const std::vector<char>& data) = 0;

    void put(std::string path, const std::string& data)
    {
        put(path, std::vector<char>(data.begin(), data.end()));
    }

    virtual bool isRemote() const { return true; }
};

typedef std::map<std::string, std::shared_ptr<Driver>> DriverMap;

} // namespace entwine

