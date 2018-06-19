/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <entwine/formats/cesium/tileset.hpp>

namespace entwine
{

void App::convert(std::vector<std::string> args)
{
    Json::Value config;
    std::size_t a(0);

    while (a < args.size())
    {
        const std::string arg(args[a]);

        if (arg == "-i")
        {
            if (++a < args.size()) config["input"] = args[a];
            else throw std::runtime_error("Invalid input");
        }
        else if (arg == "-o")
        {
            if (++a < args.size()) config["output"] = args[a];
            else throw std::runtime_error("Invalid output");
        }
        else if (arg == "-a")
        {
            if (++a < args.size()) config["tmp"] = args[a];
            else throw std::runtime_error("Invalid tmp");
        }
        else if (arg == "-t")
        {
            if (++a < args.size())
            {
                config["threads"] = Json::UInt64(std::stoul(args[a]));
            }
            else throw std::runtime_error("Invalid threads");
        }
        else if (arg == "-g")
        {
            if (++a < args.size())
            {
                config["geometricErrorDivisor"] = std::stod(args[a]);
            }
            else throw std::runtime_error("Invalid geometric error divisor");
        }
        else throw std::runtime_error("Invalid argument: " + arg);

        ++a;
    }

    cesium::Tileset tileset(config);

    std::cout << "Converting:" << std::endl;
    std::cout << "\tInput:  " << tileset.in().prefixedRoot() << "\n";
    std::cout << "\tOutput: " << tileset.out().prefixedRoot() << "\n";
    std::cout << "\tTmp:    " << tileset.tmp().prefixedRoot() << "\n";
    std::cout << "\tThreads: " << tileset.threadPool().numThreads() << "\n";

    std::cout << "Running..." << std::endl;
    tileset.build();
    std::cout << "\tDone." << std::endl;
}

} // namespace entwine

