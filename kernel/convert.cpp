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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/old-hierarchy.hpp>

using namespace entwine;

namespace
{
    std::string getUsageString()
    {
        return
            "\tUsage: entwine convert <path> <options>\n"
            "\tOptions:\n"

            "\t\t-u <aws-user>\n"
            "\t\t\tSpecify AWS credential user, if not default\n";
    }
}

void Kernel::convert(std::vector<std::string> args)
{
    if (args.size() < 1)
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Conversion path required");
    }

    const std::string path(args[0]);

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
                throw std::runtime_error("Invalid credential path argument");
            }
        }

        ++a;
    }

    Json::Value arbiterConfig;
    arbiterConfig["s3"]["profile"] = user;

    auto arbiter(std::make_shared<entwine::arbiter::Arbiter>(arbiterConfig));

    const std::string metaPath(arbiter::util::join(path, "entwine"));
    const std::string hierPath(arbiter::util::join(path, "h/"));
    const std::string backPath(arbiter::util::join(path, "h-old/"));

    // Back up metadata file, which will be altered.
    arbiter->copy(metaPath, arbiter::util::join(path, "entwine-old"));

    // Back up entire old-style hierarchy directory contents, which would
    // otherwise have some files overwritten.
    arbiter->copy(hierPath, backPath);

    Json::Value jsonMeta(parse(arbiter->get(metaPath)));

    // Naming convention conversions.
    jsonMeta["bounds"] = jsonMeta["bbox"];
    jsonMeta["boundsConforming"] = jsonMeta["bboxConforming"];

    const Structure treeStructure(jsonMeta["structure"]);
    jsonMeta["hierarchyStructure"] =
        Hierarchy::structure(treeStructure).toJson();

    // OldHierarchy old(jsonMeta["hierarchy"], arbiter->getEndpoint(backPath));
}

