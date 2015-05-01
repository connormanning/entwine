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

#include <string>
#include <vector>

#include <entwine/drivers/driver.hpp>

namespace entwine
{

class Source
{
public:
    Source(std::string rawPath, Driver& driver);
    ~Source();

    std::vector<char> getRoot();
    std::vector<char> get(std::string subpath);
    std::string getAsString(std::string subpath);
    void put(std::string path, const std::vector<char>& data);
    void put(std::string path, const std::string& data);
    bool isRemote() const;

    std::string resolve(const std::string subpath) const;

    std::string type() const;
    std::string path() const;

    static std::string getType(std::string path);

private:
    std::string m_type;
    std::string m_path;

    Driver& m_driver;
};

} // namespace entwine

