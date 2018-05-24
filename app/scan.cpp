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

#include <cstddef>
#include <string>

#include <entwine/builder/config.hpp>
#include <entwine/builder/scan.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/util/matrix.hpp>

using namespace entwine;

namespace
{
    std::string getUsageString()
    {
        return
            "\nUsage: entwine scan <path or glob> <options>\n"

            "\nPath or glob:\n"
            "\tA single file or wildcard directory path.  A non-recursive\n"
            "\tglob is signified by a single '*', e.g. \"/my/dir/*\", and a\n"
            "\trecursive search with two, e.g. \"/my/dir/*\"."

            "\nOptions:\n"

            "\t-r (<input reprojection>) <output reprojection>\n"
            "\t\tSet the spatial reference system reprojection.  The input\n"
            "\t\tvalue may be omitted to scan the input SRS from the file\n"
            "\t\theader.  In this case the build will fail if no input SRS\n"
            "\t\tmay be inferred.  Reprojection strings may be any of the\n"
            "\t\tformats supported by GDAL.\n\n"
            "\t\tIf an input reprojection is supplied, by default it will\n"
            "\t\tonly be used when no SRS can be inferred from the file.  To\n"
            "\t\toverride this behavior and use the specified input SRS even\n"
            "\t\twhen one can be found from the file header, set the '-h'\n"
            "\t\tflag.\n\n"

            "\t-o <output-path>\n"
            "\t\tIf provided, detailed per-file information will be written\n"
            "\t\tto this file in JSON format.\n\n"

            "\t-h\n"
            "\t\tIf set, the user-supplied input SRS will always override\n"
            "\t\tany SRS inferred from file headers.\n\n"

            "\t-t <threads>\n"
            "\t\tSet the number of threads.  Default: 4.\n\n"

            "\t-u <aws user>\n"
            "\t\tSpecify AWS credential user, if not default\n\n"

            "\t-a <tmp path>\n"
            "\t\tDirectory for entwine-generated temporary files.\n\n"

            "\t-x\n"
            "\t\tDo not trust file headers when determining bounds.  By\n"
            "\t\tdefault, the headers are considered to be good.\n\n";
    }
}

void App::scan(std::vector<std::string> args)
{
    if (args.empty())
    {
        std::cout << getUsageString() << std::flush;
        return;
    }

    if (args.size() == 1)
    {
        if (args[0] == "help" || args[0] == "-h" || args[0] == "--help")
        {
            std::cout << getUsageString() << std::flush;
            return;
        }
    }

    Paths paths;
    bool addingPath(args.front().front() != '-');

    Json::Value json;

    std::size_t a(0);

    while (a < args.size())
    {
        const std::string arg(args[a]);

        if (addingPath)
        {
            if (arg.front() != '-')
            {
                json["input"].append(arg);
            }
            else
            {
                addingPath = false;
            }
        }

        if (arg == "-i")
        {
            addingPath = true;
        }
        else if (arg == "-a")
        {
            if (++a < args.size())
            {
                json["tmp"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid tmp specification");
            }
        }
        else if (arg == "-o")
        {
            if (++a < args.size())
            {
                json["output"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid output specification");
            }
        }
        else if (arg == "-r")
        {
            if (++a < args.size())
            {
                const bool onlyOutput(
                        a + 1 >= args.size() ||
                        args[a + 1].front() == '-');

                if (onlyOutput)
                {
                    json["reprojection"]["out"] = args[a];
                }
                else
                {
                    json["reprojection"]["in"] = args[a];
                    json["reprojection"]["out"] = args[++a];
                }
            }
            else
            {
                throw std::runtime_error("Invalid reprojection argument");
            }
        }
        else if (arg == "-h")
        {
            json["reprojection"]["hammer"] = true;
        }
        else if (arg == "-x")
        {
            json["trustHeaders"] = false;
        }
        else if (arg == "-t")
        {
            if (++a < args.size())
            {
                json["threads"] = Json::UInt64(std::stoul(args[a]));
            }
            else
            {
                throw std::runtime_error("Invalid thread count specification");
            }
        }
        else if (arg == "-u")
        {
            if (++a < args.size())
            {
                json["arbiter"]["s3"]["profile"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid AWS user argument");
            }
        }
        else if (arg == "-e") { json["arbiter"]["s3"]["sse"] = true; }
        else if (arg == "-v") { json["arbiter"]["verbose"] = true; }

        ++a;
    }

    if (json.isMember("tmp"))
    {
        entwine::arbiter::fs::mkdirp(json["tmp"].asString());
    }

    auto arbiter(std::make_shared<entwine::arbiter::Arbiter>(json["arbiter"]));

    Scan scan(json);
    const Config in(scan.inConfig());

    std::cout << "Scanning:" << std::endl;

    if (in["input"].size() == 1)
    {
        std::cout << "\tInput: " << in["input"][0].asString() << std::endl;
    }
    else
    {
        std::cout << "\tInput: " << in["input"].size() << " files" << std::endl;
    }

    std::cout << "\tTemp path: " << in.tmp() << std::endl;
    std::cout << "\tThreads: " << in.totalThreads() << std::endl;
    // std::cout << "\tReprojection: " << reprojString << std::endl;
    std::cout << "\tTrust file headers? " << yesNo(in.trustHeaders()) <<
        std::endl;

    std::cout << std::endl;
    const Config out(scan.go());
    std::cout << std::endl;

    if (out.output().size())
    {
        std::string path(out.output());
        if (arbiter::Arbiter::getExtension(path) != "json") path += ".json";

        std::cout << "Writing details to " << path << "...";
        Json::Value json(out.json());
        arbiter->put(path, json.toStyledString());
        std::cout << " written." << std::endl;
    }

    // std::cout << out.json() << std::endl;
    std::cout << "Results:" << std::endl;
    std::cout << "\tSchema: " << getDimensionString(Schema(out["schema"])) <<
        std::endl;
    std::cout << "\tPoints: " << commify(out.numPoints()) << std::endl;
    std::cout << "\tBounds: " << Bounds(out["bounds"]) << std::endl;
    if (out.json().isMember("scale"))
    {
        std::cout << "\tScale: " << Scale(out["scale"]) << std::endl;
    }
    const double density(densityLowerBound(out.input()));
    std::cout << "\tDensity estimate (per square unit): " << density <<
        std::endl;

    /*
    if (reprojection)
    {
        std::cout << "Reprojection: " << *reprojection << std::endl;
    }
    */

    std::cout << std::endl;
}

