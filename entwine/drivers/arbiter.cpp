/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/drivers/arbiter.hpp>

#include <entwine/drivers/driver.hpp>
#include <entwine/drivers/fs.hpp>

namespace entwine
{

Arbiter::Arbiter(DriverMap drivers)
    : m_drivers{{ "fs", std::make_shared<FsDriver>(FsDriver()) }}
{
    m_drivers.insert(drivers.begin(), drivers.end());
}

Arbiter::~Arbiter()
{ }

Source Arbiter::getSource(const std::string path)
{
    return Source(path, getDriver(path));
}

Driver& Arbiter::getDriver(const std::string path)
{
    return *m_drivers.at(Source::getType(path));
}

} // namespace entwine

