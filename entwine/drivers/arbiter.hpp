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
#include <memory>
#include <string>

#include <entwine/drivers/source.hpp>

namespace entwine
{

typedef std::map<std::string, std::shared_ptr<Driver>> DriverMap;

class Arbiter
{
public:
    Arbiter(DriverMap drivers = DriverMap());
    Arbiter(const Arbiter& other);
    ~Arbiter();

    Source getSource(std::string path);

private:
    Driver& getDriver(std::string path);

    DriverMap m_drivers;
};

} // namespace entwine

