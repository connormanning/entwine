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

#include <stdexcept>
#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/third/arbiter/arbiter.hpp>

class Kernel
{
public:
    static void build(std::vector<std::string> args);
    static void merge(std::vector<std::string> args);
    static void infer(std::vector<std::string> args);
    static void rebase(std::vector<std::string> args);
};

