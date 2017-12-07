/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/json.hpp>

using namespace entwine;

namespace
{
    std::string getUsageString()
    {
        return "\tUsage: entwine rebase <path> <depth (default: 9)>\n";
    }

    void rebase(std::string path, std::size_t depth)
    {
        std::cout << "Rebasing " << path << " to " << depth << std::endl;

        arbiter::Arbiter a;
        auto ep(a.getEndpoint(path));
        Metadata m(ep);

        auto backup([&a, &ep, &m](std::string file)
        {
            a.copy(
                    ep.prefixedRoot() + file,
                    ep.prefixedRoot() + file + "-rebase-backup-" +
                        std::to_string(m.hierarchyStructure().baseDepthEnd()),
                    true);
            a.copy(
                    ep.prefixedRoot() + file,
                    ep.prefixedRoot() + file + "-rebase-backup",
                    true);
        });

        backup("entwine");
        backup("h/ids");
        backup("h/0");

        HierarchyCell::Pool pool;
        Hierarchy h(pool, m, ep, &ep, true, false);
        h.rebase(ep, depth);
    }
}

void Kernel::rebase(std::vector<std::string> args)
{
    if (args.size() < 1)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Rebase path required");
    }
    if (args.size() > 2)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Invalid arguments");
    }

    const std::string pathArg(args[0]);
    const std::size_t depth(
            args.size() == 2 ? std::stoul(args[1]) : 9);

    std::vector<std::string> paths;

    const std::string postfix(".json");
    if (
            pathArg.size() > postfix.size() &&
            pathArg.substr(pathArg.size() - postfix.size()) == postfix)
    {
        arbiter::Arbiter a;
        const Json::Value json(parse(a.get(pathArg)));
        if (!json.isArray())
        {
            throw std::runtime_error("JSON must be an array of paths");
        }

        paths = extract<std::string>(json);
    }
    else
    {
        paths.push_back(pathArg);
    }

    for (const std::string path : paths)
    {
        try
        {
            ::rebase(path, depth);
        }
        catch (std::exception& e)
        {
            std::cout << "Failed to rebase " << path << ": " << e.what() <<
                std::endl;
        }
    }

    std::cout << "Done" << std::endl;
}

