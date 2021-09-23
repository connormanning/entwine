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

#include <entwine/types/metadata.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/config.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/info.hpp>
#include <entwine/util/time.hpp>

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

    m_ap.add(
        "--summary",
        "Filename for a JSON summary of the analysis",
        [this](json j) { m_json["summary"] = j.get<std::string>(); });

    addOutput(
            "If provided, detailed per-file information will be written "
            "to this directory in JSON format\n"
            "Example: --output my-output/");

    addTmp();
    addDeep();
    addReprojection();
    addSimpleThreads();
    addConfig();
    addArbiter();
}

void Info::run()
{
    const arbiter::Arbiter a = config::getArbiter(m_json);
    StringList inputs = config::getInput(m_json);
    if (inputs.empty())
    {
        std::cout << "No inputs supplied - exiting" << std::endl;
        return;
    }
    if (std::any_of(inputs.begin(), inputs.end(), isDirectory))
    {
        std::cout << "Resolving inputs..." << std::endl;
        inputs = resolve(inputs, a);
        std::cout << "\tResolved." << std::endl;
    }

    const std::string output = config::getOutput(m_json);
    const std::string tmp = config::getTmp(m_json);
    const bool deep = config::getDeep(m_json);
    const unsigned threads = config::getThreads(m_json);
    const json pipeline = config::getPipeline(m_json);
    const auto reprojection = config::getReprojection(m_json);
    const std::string summaryFilename = m_json.value("summary", "");

    if (inputs.empty()) throw std::runtime_error("No files found!");

    std::cout << "Analyzing:\n" <<
        "\tInput: " <<
            (inputs.size() > 1
                ? std::to_string(inputs.size()) + " paths"
                : inputs.at(0)) << "\n" <<
        (output.size() ? "\tOutput: " + output + "\n" : "") <<
        "\tReprojection: " << getReprojectionString(reprojection) << "\n" <<
        "\tType: " << (deep ? "deep" : "shallow") << "\n" <<
        "\tThreads: " << threads << "\n" <<
        std::endl;

    const SourceList sources = analyze(
        inputs,
        pipeline,
        deep,
        tmp,
        a,
        threads);
    const SourceInfo summary = manifest::reduce(sources);

    std::cout << "\tDone.\n" << std::endl;

    if (!summary.points)
    {
        printProblems(summary.warnings, summary.errors);
        throw std::runtime_error("No points found!");
    }

    printInfo(
        summary.schema,
        summary.bounds,
        summary.srs,
        summary.points,
        summary.warnings,
        summary.errors);
    std::cout << std::endl;

    if (output.size())
    {
        std::cout << "Saving output..." << std::endl;
        const bool pretty = sources.size() <= 1000;
        const auto endpoint = a.getEndpoint(output);
        saveMany(sources, endpoint, threads, pretty);
        std::cout << "\tSaved." << std::endl;
    }

    if (summaryFilename.size())
    {
        std::cout << "Saving summary..." << std::endl;
        a.put(summaryFilename, json(summary).dump(2));
    }
}

} // namespace app
} // namespace entwine
