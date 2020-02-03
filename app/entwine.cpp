/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "build.hpp"
#include "entwine.hpp"
#include "convert.hpp"
#include "merge.hpp"
#include "scan.hpp"

#include <csignal>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/stack-trace.hpp>

namespace
{
    std::string t(uint64_t n) { return std::string(n * 4, ' '); }
    std::string getUsageString()
    {
        return
            t(1) + "Version: " + entwine::currentEntwineVersion().toString() +
                "\n" +
            t(1) + "Usage: entwine <app> <options>\n" +
            t(1) + "Apps:\n" +
            t(2) + "build\n" +
            t(3) + "Build an EPT dataset\n" +
            t(2) + "scan\n" +
            t(3) + "Aggregate information about an unindexed dataset\n" +
            t(2) + "merge\n" +
            t(3) + "Merge colocated entwine subsets\n" +
            t(2) + "convert\n" +
            t(3) + "Convert an entwine dataset to a different format\n";
    }

    std::mutex mutex;
}

namespace entwine
{
namespace app
{

void App::addInput(std::string description, const bool asDefault)
{
    auto f([this](json j)
    {
        if (j.is_array())
        {
            for (const json& entry : j)
            {
                m_json["input"].push_back(entry);
            }
        }
        else m_json["input"].push_back(j);
    });

    if (asDefault) m_ap.addDefault("--input", "-i", description, f);
    else m_ap.add("--input", "-i", description, f);
}

void App::addOutput(std::string description, const bool asDefault)
{
    auto f([this](json j) { m_json["output"] = j; });

    if (asDefault) m_ap.addDefault("--output", "-o", description, f);
    else m_ap.add("--output", "-o", description, f);
}

void App::addConfig()
{
    m_ap.add(
            "--config",
            "-c",
            "A configuration file.  Subsequent options will override "
            "configuration file parameters, so it may be used for templating "
            "common options among multiple runs.\n"
            "Example: --config template.json -i in.laz -o out",
            [this](json j)
            {
                arbiter::Arbiter a(m_json.value("arbiter", json()).dump());
                m_json = merge(
                        m_json,
                        json::parse(a.get(j.get<std::string>())));
            });
}

void App::addTmp()
{
    m_ap.add(
            "--tmp",
            "-a",
            "Directory for entwine-generated temporary files\n"
            "Example: --tmp /tmp/entwine",
            [this](json j) { m_json["tmp"] = j; });
}

void App::addSimpleThreads()
{
    m_ap.add(
            "--threads",
            "-t",
            "Set the number of threads\n"
            "Example: --threads 12",
            [this](json j) { m_json["threads"] = extract(j); });
}

void App::addReprojection()
{
    m_ap.add(
            "--srs",
            "Set the `srs` metadata entry of the output.  If reprojecting, "
            "this value will be set automatically from the output projection.  "
            "Typically this value is automatically inferred from the files "
            "themselves.",
            [this](json j) { m_json["srs"] = Srs(j); });

    m_ap.add(
            "--reprojection",
            "-r",
            "Set the SRS reprojection.  The input SRS may be omitted to "
            "use values from the file headers.  By default, SRS values "
            "found in file headers will override the input SRS.  To always "
            "use the input SRS regardless of file headers, see the "
            "--hammer option\n"
            "Example: --reprojection EPSG:3857, -r EPSG:26915 EPSG:3857",
            [this](json j)
            {
                if (j.is_string())
                {
                    m_json["reprojection"]["out"] = j;
                }
                else if (j.is_array() && j.size() == 2)
                {
                    m_json["reprojection"]["in"] = j[0];
                    m_json["reprojection"]["out"] = j[1];
                }
                else
                {
                    throw std::runtime_error(
                            "Invalid reprojection: " + j.dump(2));
                }
            });

    m_ap.add(
            "--hammer",
            "-h",
            "If set, the user-supplied input SRS (see --reprojection) will "
            "always override any SRS found in file headers.  An input "
            "SRS is required if this option is set.\n"
            "Example: --reprojection EPSG:26915 EPSG:3857 --hammer",
            [this](json j)
            {
                checkEmpty(j);
                m_json["reprojection"]["hammer"] = true;
            });
}

void App::addNoTrustHeaders()
{
    m_ap.add(
            "--noTrustHeaders",
            "-x",
            "If set, do not trust file headers when determining bounds, "
            "instead read every point",
            [this](json j)
            {
                checkEmpty(j);
                m_json["trustHeaders"] = false;
            });
}

void App::addAbsolute()
{
    m_ap.add(
            "--absolute",
            "If set, absolutely positioned XYZ coordinates will be used "
            "instead of scaled values",
            [this](json j)
            {
                checkEmpty(j);
                m_json["absolute"] = true;
            });
}

void App::addArbiter()
{
    m_ap.add(
            "--profile",
            "-p",
            "Specify AWS user profile, if not default\n"
            "Example: --profile john",
            [this](json j) { m_json["arbiter"]["s3"]["profile"] = j; });

    m_ap.add(
            "--sse",
            "Enable AWS server-side encryption",
            [this](json j)
            {
                checkEmpty(j);
                m_json["arbiter"]["s3"]["sse"] = true;
            });

    m_ap.add(
            "--requester-pays",
            "Set the requester-pays flag to S3\n",
            [this](json j)
            {
                checkEmpty(j);
                m_json["arbiter"]["s3"]["requesterPays"] = true;
            });

    m_ap.add(
            "--allow-instance-profile",
            "Allow EC2 instance profile use for S3 backends\n",
            [this](json j)
            {
                checkEmpty(j);
                m_json["arbiter"]["s3"]["allowInstanceProfile"] = true;
            });

    m_ap.add(
            "--verbose",
            "-v",
            "Enable developer-level verbosity",
            [this](json j)
            {
                checkEmpty(j);
                m_json["arbiter"]["verbose"] = true;
            });
}

std::string App::getDimensionString(const Schema& schema) const
{
    const DimList dims(schema.dims());
    std::string results("[\n");
    const std::string prefix(16, ' ');
    const std::size_t width(80);

    std::string line;

    for (std::size_t i(0); i < dims.size(); ++i)
    {
        const auto name(dims[i].name());
        const auto type(dims[i].typeName());
        const bool last(i == dims.size() - 1);

        if (prefix.size() + line.size() + name.size() + 1 >= width)
        {
            results += prefix + line + '\n';
            line.clear();
        }

        if (line.size()) line += ' ';
        line += dims[i].name() + ':' + type;

        if (!last) line += ',';
        else results += prefix + line + '\n';
    }

    results += "\t]";

    return results;
}

} // namespace app
} // namespace entwine

int main(int argc, char** argv)
{
    // Since we use entrypoint for docker, we need to explicitly listen for
    // this so that Ctrl+C will work in that context.
    signal(SIGINT, [](int sig) { exit(1); });
    entwine::stackTraceOn(SIGSEGV);

    // We ignore SIGPIPE to prevent crashing in some cases
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2)
    {
        std::cout << "App type required\n" << getUsageString() << std::endl;
        exit(1);
    }

    const std::string app(argv[1]);

    std::vector<std::string> args;

    for (int i(2); i < argc; ++i)
    {
        args.push_back(argv[i]);
    }

    try
    {
        if (app == "scan")
        {
            entwine::app::Scan().go(args);
        }
        else if (app == "build")
        {
            entwine::app::Build().go(args);
        }
        else if (app == "merge")
        {
            entwine::app::Merge().go(args);
        }
        else if (app == "convert")
        {
            entwine::app::Convert().go(args);
        }
        else
        {
            if (app != "help" && app != "-h" && app != "--help")
            {
                std::cout << "Invalid app type\n";
            }

            std::cout << getUsageString() << std::endl;
        }
    }
    catch (std::runtime_error& e)
    {
        std::cout << "Encountered an error: " << e.what() << std::endl;
        std::cout << "Exiting." << std::endl;
        return 1;
    }

    return 0;
}

