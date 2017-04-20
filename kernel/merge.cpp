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

#include <entwine/tree/merger.hpp>

using namespace entwine;

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

void Kernel::merge(std::vector<std::string> args)
{
    if (args.size() < 1)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Merge path required");
    }

    const std::string path(args[0]);

    std::size_t a(1);
    std::size_t threads(1);

    Json::Value arbiterConfig;

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-u")
        {
            if (++a < args.size())
            {
                arbiterConfig["s3"]["profile"] = args[a];
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
                threads = std::stoul(args[a]);
            }
            else
            {
                throw std::runtime_error("Invalid credential path argument");
            }
        }

        ++a;
    }

    auto arbiter(std::make_shared<entwine::arbiter::Arbiter>(arbiterConfig));

    Merger merger(path, threads, true, arbiter);

    std::cout << "Merging " << path << "..." << std::endl;
    merger.go();
    std::cout << "Merge complete." << std::endl;
}

