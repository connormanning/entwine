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

#include <csignal>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <entwine/types/defs.hpp>
#include <entwine/util/stack-trace.hpp>

namespace
{
    std::string getUsageString()
    {
        return
            "\tVersion: " + entwine::currentVersion().toString() + "\n" +
            "\tUsage: entwine <app> <options>\n"
            "\tApps:\n"
            "\t\tbuild\n"
            "\t\t\tBuild (or continue to build) an index\n"
            "\t\tscan\n"
            "\t\t\tAggregate information for an unindexed dataset\n"
            "\t\tmerge\n"
            "\t\t\tMerge colocated previously built subsets\n";
    }

    std::mutex mutex;
}

int main(int argc, char** argv)
{
    // Since we use entrypoint for docker, we need to explicitly listen for
    // this so that Ctrl+C will work in that context.
    signal(SIGINT, [](int sig) { exit(1); });
    entwine::stackTraceOn(SIGSEGV);

    if (argc < 2)
    {
        std::cout << "App type required\n" << getUsageString() << std::endl;
        exit(1);
    }

    const std::string app(argv[1]);

    std::vector<std::string> args;

    for (int i(2); i < argc; ++i)
    {
        std::string arg(argv[i]);

        if (arg.size() > 2 && arg.front() == '-' && std::isalpha(arg[1]))
        {
            // Expand args of the format "-xvalue" to "-x value".
            args.push_back(arg.substr(0, 2));
            args.push_back(arg.substr(2));
        }
        else
        {
            args.push_back(argv[i]);
        }
    }

    try
    {
        if (app == "build")
        {
            entwine::App::build(args);
        }
        else if (app == "merge")
        {
            entwine::App::merge(args);
        }
        else if (app == "scan")
        {
            entwine::App::scan(args);
        }
        else if (app == "convert")
        {
            entwine::App::convert(args);
        }
        else
        {
            if (app != "help" && app != "-h" && app != "--help")
            {
                std::cout << "Invalid app type\n";
            }

            std::cout << getUsageString() << std::endl;
        }
    }
    catch (std::runtime_error& e)
    {
        std::cout << "Encountered an error: " << e.what() << std::endl;
        std::cout << "Exiting." << std::endl;
        return 1;
    }

    return 0;
}

