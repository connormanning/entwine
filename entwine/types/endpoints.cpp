/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/endpoints.hpp>

namespace entwine
{

namespace
{

void create(const std::string& dir)
{
    if (!arbiter::mkdirp(dir))
    {
        throw std::runtime_error("Failed to create directory: " + dir);
    }
}

} // unnamed namespace

Endpoints::Endpoints(
    const json a,
    const std::string outputPath,
    const std::string tmpPath)
    : arbiter(std::make_shared<arbiter::Arbiter>(a.dump()))
    // , arbiter(*sharedArbiter)
    , output(arbiter->getEndpoint(outputPath))
    , data(output.getSubEndpoint("ept-data"))
    , hierarchy(output.getSubEndpoint("ept-hierarchy"))
    , sources(output.getSubEndpoint("ept-sources"))
    , tmp(arbiter->getEndpoint(tmpPath))
{
    if (!tmp.isLocal())
    {
        throw std::runtime_error("Temporary path must be local");
    }

    create(tmpPath);

    if (output.isLocal())
    {
        create(outputPath);
        create(arbiter::join(outputPath, "ept-data"));
        create(arbiter::join(outputPath, "ept-hierarchy"));
        create(arbiter::join(outputPath, "ept-sources"));
    }
}

} // namespace entwine
