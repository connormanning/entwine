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

#include <entwine/third/arbiter/arbiter.hpp>
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

            "\t\t-u <aws-user>\n"
            "\t\t\tSpecify AWS credential user, if not default\n";
    }
}

void Kernel::link(std::vector<std::string> args)
{
    if (args.size() < 5)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Not enough arguments");
    }

    const std::string path(args[0]);
    std::vector<std::string> subs;

    std::string user;

    std::size_t a(1);

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-u")
        {
            if (++a < args.size())
            {
                user = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid AWS user argument");
            }
        }
        else
        {
            subs.push_back(arg);
        }

        ++a;
    }

    if (!(subs.size() == 4 || subs.size() == 16 || subs.size() == 64))
    {
        throw std::runtime_error("Invalid number of subsets");
    }

    std::shared_ptr<arbiter::Arbiter> arbiter(
            std::make_shared<arbiter::Arbiter>(user));

    Builder builder(path, arbiter);

    std::cout << "Linking " << subs.size() << " paths..." << std::endl;
    builder.link(subs);
    std::cout << "Done." << std::endl;
}

