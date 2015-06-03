/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <fstream>
#include <iostream>

#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/builder.hpp>

using namespace entwine;

namespace
{
    std::string getUsageString()
    {
        return
            "\tUsage: entwine link <output path> <subset path 1> ... "
                "<subset path N> <options>\n"
            "\tOptions:\n"

            "\t\t-c <credentials-path.json>\n"
            "\t\t\tSpecify path to AWS S3 credentials\n";
    }
}

void Kernel::link(std::vector<std::string> args)
{
    if (args.size() != 1)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Merge path required");
    }

    const std::string path(args[0]);

    std::string credPath("credentials.json");

    int a(1);

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-c")
        {
            if (++a < args.size())
            {
                credPath = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid credential path argument");
            }
        }
    }

    DriverMap drivers;

    {
        std::unique_ptr<AwsAuth> auth(getCredentials(credPath));
        if (auth)
        {
            drivers.insert({ "s3", std::make_shared<S3Driver>(*auth) });
        }
    }

    std::shared_ptr<Arbiter> arbiter(std::make_shared<Arbiter>(drivers));
}

