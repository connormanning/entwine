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

#include <execinfo.h>
#include <unistd.h>

namespace
{
    void handler(int sig)
    {
        void* array[16];
        const std::size_t size(backtrace(array, 16));

        std::cout << "Got error " << sig << std::endl;
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        exit(1);
    }

    std::string getUsageString()
    {
        return
            "\tUsage: entwine <kernel> <options>\n"
            "\tKernels:\n"
            "\t\tbuild\n"
            "\t\t\tBuild (or continue to build) an index\n"
            "\t\tinfer\n"
            "\t\t\tAggregate information for an unindexed dataset\n"
            "\t\tmerge\n"
            "\t\t\tMerge colocated previously built subsets\n";
    }
}

int main(int argc, char** argv)
{
    signal(SIGSEGV, handler);

    if (argc < 2)
    {
        std::cout << "Kernel type required\n" << getUsageString() << std::endl;
        exit(1);
    }

    const std::string kernel(argv[1]);

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
        if (kernel == "build")
        {
            Kernel::build(args);
        }
        else if (kernel == "merge")
        {
            Kernel::merge(args);
        }
        else if (kernel == "infer")
        {
            Kernel::infer(args);
        }
        else if (kernel == "convert")
        {
            Kernel::convert(args);
        }
        else
        {
            if (kernel != "help" && kernel != "-h" && kernel != "--help")
            {
                std::cout << "Invalid kernel type\n";
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

