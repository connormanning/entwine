/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include "entwine.hpp"

namespace entwine
{
namespace app
{

class Merge : public App
{
private:
    virtual void addArgs() override;
    virtual void run() override;
};

} // namespace app
} // namespace entwine

