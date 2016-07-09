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
    bool recover(false);

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
        else if (arg == "-recover")
        {
            recover = true;
        }

        ++a;
    }

    if (!recover) std::cout << "Converting " << path << std::endl;
    else std::cout << "RECOVERING " << path << std::endl;

    Json::Value arbiterConfig;
    arbiterConfig["s3"]["profile"] = user;

    auto arbiter(std::make_shared<entwine::arbiter::Arbiter>(arbiterConfig));
    const auto topEp(arbiter->getEndpoint(path));

    const std::string metaPath(arbiter::util::join(path, "entwine"));
    const std::string hierPath(arbiter::util::join(path, "h/"));
    const std::string backPath(arbiter::util::join(path, "h-old/"));

    if (recover)
    {
        std::cout << "Reverting main metadata" << std::endl;
        arbiter->copy(
                arbiter::util::join(path, "entwine-old"),
                metaPath);

        std::cout << "Reverting hierarchy" << std::endl;
        arbiter->copy(backPath, hierPath);

        std::cout << "Recovered " << path << std::endl;

        return;
    }

    // Back up metadata file, which will be altered.
    arbiter->copy(metaPath, arbiter::util::join(path, "entwine-old"), true);
    std::cout << "Backed up main metadata" << std::endl;

    // Back up entire old-style hierarchy directory contents, which would
    // otherwise have some files overwritten.
    arbiter->copy(hierPath, backPath, true);
    std::cout << "Backed up hierarchy" << std::endl;

    Json::Value jsonMeta(parse(arbiter->get(metaPath)));

    // Naming convention conversions.
    jsonMeta["bounds"] = jsonMeta["bbox"];
    jsonMeta["boundsConforming"] = jsonMeta["bboxConforming"];
    jsonMeta["structure"]["pointsPerChunk"] =
        jsonMeta["structure"]["chunkPoints"];

    // Convert top-level keys to new Format layout, and add the tailFields.
    jsonMeta["format"]["srs"] = jsonMeta["srs"];
    jsonMeta["format"]["trustHeaders"] = jsonMeta["trustHeaders"];
    jsonMeta["format"]["compress"] = jsonMeta["compressed"];
    jsonMeta["format"]["tail"].append("numPoints");
    jsonMeta["format"]["tail"].append("chunkType");

    // Add parameter description of new-style hierarchy.
    const Structure treeStructure(jsonMeta["structure"]);
    jsonMeta["hierarchyStructure"] =
        Hierarchy::structure(treeStructure).toJson();

    const Json::Value oldHierarchyMeta(jsonMeta["hierarchy"]);

    std::cout << "Awakening old hierarchy" << std::endl;
    OldHierarchy oldHierarchy(oldHierarchyMeta, arbiter->getEndpoint(backPath));
    oldHierarchy.awakenAll();

    std::cout << "Initialized old hierarchy" << std::endl;

    const Metadata metadata(jsonMeta);
    std::cout << "Initialized converted metadata" << std::endl;
    metadata.save(topEp);
    std::cout << "Saved new metadata" << std::endl;

    Hierarchy newHierarchy(metadata, topEp);
    std::cout << "Converting hierarchy" << std::endl;
    oldHierarchy.insertInto(newHierarchy, metadata);
    newHierarchy.save(topEp);

    std::cout << "Saved new hierarchy" << std::endl;
    std::cout << "All done" << std::endl;
}

