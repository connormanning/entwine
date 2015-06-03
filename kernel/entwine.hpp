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

#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/s3.hpp>
#include <entwine/third/json/json.h>

class Kernel
{
public:
    static void build(std::vector<std::string> args);
    static void merge(std::vector<std::string> args);
    static void link(std::vector<std::string> args);

private:
    static std::unique_ptr<entwine::AwsAuth> getCredentials(
            const std::string credPath);
};

