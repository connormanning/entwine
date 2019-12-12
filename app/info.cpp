/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "info.hpp"

#include <entwine/util/info.hpp>

namespace entwine
{
namespace app
{

void Info::addArgs()
{
    m_ap.setUsage("entwine info <path(s)> (<options>)");

    addInput(
            "File paths or directory entries.  For a recursive directory "
            "search, the notation is 'directory/**'\n"
            "Example: --input path.laz, --input data-directory/",
            true);

    addOutput(
            "If provided, detailed per-file information will be written "
            "to this directory in JSON format\n"
            "Example: --output my-output/");

    addConfig();
    addTmp();
    addReprojection();
    addSimpleThreads();
    addNoTrustHeaders();
    addArbiter();
}

void Info::run()
{
    const auto sources = analyze(m_json);
    const auto reduced = reduce(sources);

    if (m_json.count("output"))
    {
        const std::string output(m_json.at("output").get<std::string>());
        const arbiter::Arbiter a(m_json.value("arbiter", json()).dump());

        if (a.isLocal(output)) arbiter::mkdirp(output);
        serialize(sources, a.getEndpoint(output));
    }

    std::cout << "Sources: " << json(sources).dump(2) << std::endl;
    std::cout << "Info: " << json(reduced).dump(2) << std::endl;
}

} // namespace app
} // namespace entwine
