/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <fstream>
#include <iostream>

#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/builder.hpp>

using namespace entwine;

namespace
{
    Json::Reader reader;

    std::unique_ptr<AwsAuth> getCredentials(const std::string credPath)
    {
        std::unique_ptr<AwsAuth> auth;

        Json::Value credentials;
        std::ifstream credFile(credPath, std::ifstream::binary);
        if (credFile.good())
        {
            reader.parse(credFile, credentials, false);

            auth.reset(
                    new AwsAuth(
                        credentials["access"].asString(),
                        credentials["hidden"].asString()));
        }

        return auth;
    }
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cout << "Merge path required." << std::endl;
    }

    const std::string path(argv[1]);

    std::string credPath("credentials.json");

    std::size_t argNum(2);

    while (argNum < argc)
    {
        std::string arg(argv[argNum++]);

        if (arg == "-c")
        {
            credPath = argv[argNum++];
        }
        else if (arg.size() > 2 && arg.substr(0, 2) == "-c")
        {
            credPath = arg.substr(2);
        }
    }

    DriverMap drivers;

    {
        std::unique_ptr<AwsAuth> auth(getCredentials(credPath));
        if (auth)
        {
            drivers.insert({ "s3", std::make_shared<S3Driver>(*auth) });
        }
    }

    std::shared_ptr<Arbiter> arbiter(std::make_shared<Arbiter>(drivers));

    Builder builder(path, arbiter);

    std::cout << "Merging..." << std::endl;
    builder.merge();
    std::cout << "Done." << std::endl;
}

