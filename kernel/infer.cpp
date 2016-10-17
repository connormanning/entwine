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

#include <entwine/types/reprojection.hpp>
#include <entwine/util/inference.hpp>

using namespace entwine;

namespace
{
    std::string getUsageString()
    {
        return
            "\nUsage: entwine infer <path or glob> <options>\n"

            "\nPath or glob:\n"
            "\tA single file or wildcard directory path.  A non-recursive\n"
            "\tglob is signified by a single '*', e.g. \"/my/dir/*\", and a\n"
            "\trecursive search with two, e.g. \"/my/dir/*\"."

            "\nOptions:\n"

            "\t-r (<input reprojection>) <output reprojection>\n"
            "\t\tSet the spatial reference system reprojection.  The input\n"
            "\t\tvalue may be omitted to infer the input SRS from the file\n"
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

    std::string getReprojString(const Reprojection* reprojection)
    {
        if (reprojection)
        {
            std::string s;

            if (reprojection->hammer())
            {
                s += reprojection->in() + " (OVERRIDING file headers)";
            }
            else
            {
                if (reprojection->in().size())
                {
                    s += "(from file headers, or a default of '";
                    s += reprojection->in();
                    s += "')";
                }
                else
                {
                    s += "(from file headers)";
                }
            }

            s += " -> ";
            s += reprojection->out();

            return s;
        }
        else
        {
            return "(none)";
        }
    }
}

void Kernel::infer(std::vector<std::string> args)
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

    std::string path;

    std::size_t threads(4);
    Json::Value jsonReprojection;
    std::string user;
    std::string tmpPath("tmp");
    bool trustHeaders(true);

    std::string output;

    std::size_t a(0);

    while (a < args.size())
    {
        const std::string arg(args[a]);

        if (arg.front() != '-')
        {
            // If this is not an option argument, use it as the path.
            if (path.empty())
            {
                path = arg;
            }
            else
            {
                throw std::runtime_error(
                        "Only one path allowed - found both '" +
                        path + "' and '" + arg + "'");
            }
        }
        else if (arg == "-a")
        {
            if (++a < args.size())
            {
                tmpPath = args[a];
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
                output = args[a];
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
                    jsonReprojection["out"] = args[a];
                }
                else
                {
                    jsonReprojection["in"] = args[a];
                    jsonReprojection["out"] = args[++a];
                }
            }
            else
            {
                throw std::runtime_error("Invalid reprojection argument");
            }
        }
        else if (arg == "-h")
        {
            jsonReprojection["hammer"] = true;
        }
        else if (arg == "-x")
        {
            trustHeaders = false;
        }
        else if (arg == "-t")
        {
            if (++a < args.size())
            {
                threads = Json::UInt64(std::stoul(args[a]));
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
                user = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid AWS user argument");
            }
        }

        ++a;
    }

    entwine::arbiter::fs::mkdirp(tmpPath);

    std::unique_ptr<Reprojection> reprojection;

    if (jsonReprojection.isMember("out"))
    {
        reprojection.reset(new Reprojection(jsonReprojection));
    }

    Json::Value arbiterConfig;
    arbiterConfig["s3"]["profile"] = user;

    auto arbiter(std::make_shared<entwine::arbiter::Arbiter>(arbiterConfig));

    const auto reprojString(getReprojString(reprojection.get()));
    const auto trustHeadersString(trustHeaders ? "yes" : "no");

    std::cout << "Inferring from: " << path << std::endl;
    std::cout << "\tTemp path: " << tmpPath << std::endl;
    std::cout << "\tThreads: " << threads << std::endl;
    std::cout << "\tReprojection: " << reprojString << std::endl;
    std::cout << "\tTrust file headers? " << trustHeadersString << std::endl;

    Inference inference(
            path,
            tmpPath,
            threads,
            true,
            reprojection.get(),
            trustHeaders,
            arbiter.get());

    inference.go();

    if (output.size())
    {
        std::cout << "Writing details to " << output << "..." << std::endl;

        Json::Value json;
        json["manifest"] = inference.manifest().toInferenceJson();
        json["schema"] = inference.schema().toJson();
        json["bounds"] = inference.nativeBounds().toJson();
        json["numPoints"] = Json::UInt64(inference.numPoints());

        if (reprojection) json["reproject"] = reprojection->toJson();

        arbiter->put(output, json.toStyledString());
    }

    std::cout << "Schema: " << inference.schema() << std::endl;
    std::cout << "Bounds: " << inference.nativeBounds() << std::endl;
    std::cout << "Points: " << inference.numPoints() << std::endl;

    if (reprojection)
    {
        std::cout << "Reprojection: " << *reprojection << std::endl;
    }

    if (const auto delta = inference.delta())
    {
        std::cout << "Scale:  " << delta->scale() << std::endl;
        std::cout << "Offset: " << delta->offset() << std::endl;
    }
}

