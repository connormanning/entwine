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

class Builder;

namespace app
{

class Build : public App
{
private:
    virtual void addArgs() override;
    virtual void run() override;

    void log(const Builder& b) const;
    void end(const Builder& b) const;
};

} // namespace app
} // namespace entwine

