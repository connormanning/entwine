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

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <entwine/drivers/source.hpp>

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

    std::vector<std::string> resolve(std::string path)
    {
        std::vector<std::string> results;

        if (path.size() > 2 && path.substr(path.size() - 2) == "/*")
        {
            std::cout << "Resolving " << path << " ..." << std::flush;
            results = glob(Source::stripType(path));
            std::cout << "\n\tResolved to " << results.size() << " paths." <<
                std::endl;
        }
        else
        {
            results.push_back(path);
        }

        return results;
    }

    virtual std::vector<std::string> glob(std::string path)
    {
        throw std::runtime_error("Cannot glob driver for: " + path);
    }

    virtual bool isRemote() const { return true; }
};

typedef std::map<std::string, std::shared_ptr<Driver>> DriverMap;

} // namespace entwine

