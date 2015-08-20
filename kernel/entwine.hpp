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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/json/json.hpp>

class Kernel
{
public:
    static void build(std::vector<std::string> args);
    static void merge(std::vector<std::string> args);
    static void link(std::vector<std::string> args);

private:
    static std::shared_ptr<arbiter::Arbiter> getArbiter(std::string credPath);
};

