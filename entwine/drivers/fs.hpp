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

#include <entwine/drivers/driver.hpp>

namespace entwine
{

class FsDriver : public Driver
{
public:
    virtual std::vector<char> get(std::string path);
    virtual void put(std::string path, const std::vector<char>& data);

    virtual bool isRemote() const { return false; }
};

} // namespace entwine

