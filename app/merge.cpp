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
#include <memory>
#include <string>

#include <json/json.h>

#include <entwine/builder/config.hpp>
#include <entwine/builder/merger.hpp>

namespace entwine
{

namespace
{
    std::string getUsageString()
    {
        return
            "\tUsage: entwine merge <path> <options>\n"
            "\tOptions:\n"

            "\t-t <threads>\n"
            "\t\tSet the number of worker threads.  Recommended to be no\n"
            "\t\tmore than the physical number of cores.\n\n"

            "\t\t-u <aws-user>\n"
            "\t\t\tSpecify AWS credential user, if not default\n";
    }
}

void App::merge(std::vector<std::string> args)
{
    if (args.size() < 1)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Merge path required");
    }

    Config config;
    config["output"] = args[0];
    config["verbose"] = true;

    std::size_t a(1);

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-u")
        {
            if (++a < args.size())
            {
                config["arbiter"]["s3"]["profile"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid credential path argument");
            }
        }
        else if (arg == "-t")
        {
            if (++a < args.size())
            {
                config["threads"] = Json::UInt64(std::stoul(args[a]));
            }
            else
            {
                throw std::runtime_error("Invalid threads setting");
            }
        }

        ++a;
    }

    Merger merger(config);

    std::cout << "Merging " << config["output"].asString() << "..." <<
        std::endl;
    merger.go();
    std::cout << "Merge complete." << std::endl;
}

} // namespace entwine

