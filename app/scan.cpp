/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "scan.hpp"

#include <cstddef>
#include <string>

#include <entwine/builder/config.hpp>
#include <entwine/builder/scan.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/matrix.hpp>

namespace entwine
{
namespace app
{

void Scan::addArgs()
{
    m_ap.setUsage("entwine scan <path(s)> (<options>)");

    addInput(
            "File paths or directory entries.  For a recursive directory "
            "search, the notation is 'directory/**'\n"
            "Example: --input path.laz, --input data-directory/",
            true);

    addOutput(
            "If provided, detailed per-file information will be written "
            "to this file in JSON format\n"
            "Example: --output scan-output.json");

    addConfig();
    addTmp();
    addReprojection();
    addSimpleThreads();
    addNoTrustHeaders();
    addAbsolute();
    addArbiter();
}

void Scan::run()
{
    m_json["verbose"] = true;

    Config in(m_json);
    entwine::Scan scan(in);
    in = scan.inConfig();
    std::unique_ptr<Reprojection> reprojection(in.reprojection());

    std::cout << "Scanning:" << std::endl;

    if (in["input"].isString())
    {
        std::cout << "\tInput: " << in["input"].asString() << std::endl;
    }
    else if (in["input"].size() == 1)
    {
        std::cout << "\tInput: " << in["input"][0].asString() << std::endl;
    }
    else
    {
        std::cout << "\tInput: " << in["input"].size() << " files" << std::endl;
    }

    std::cout << "\tThreads: " << in.totalThreads() << std::endl;
    std::cout << "\tReprojection: " <<
        (reprojection ? reprojection->toString() : "(none)") << std::endl;
    std::cout << "\tTrust file headers? " << yesNo(in.trustHeaders()) << "\n" <<
        std::endl;

    const Config out(scan.go());
    const Schema schema(out.schema());
    std::cout << std::endl;

    std::cout << "Results:" << std::endl;
    std::cout << "\tSchema: " << getDimensionString(schema) << std::endl;
    std::cout << "\tPoints: " << commify(out.points()) << std::endl;
    std::cout << "\tBounds: " << Bounds(out["bounds"]) << std::endl;

    std::cout << "\tScale: ";
    if (schema.isScaled())
    {
        Scale s(schema.scale());
        if (s.x == s.y && s.x == s.z) std::cout << s.x;
        else std::cout << s;
    }
    else std::cout << "(absolute)";
    std::cout << std::endl;

    const double density(densityLowerBound(out.input()));
    std::cout << "\tDensity estimate (per square unit): " << density <<
        std::endl;

    std::cout << "\tSpatial reference: ";
    const Srs srs(out.srs());
    if (srs.hasCode()) std::cout << srs.codeString() << std::endl;
    else std::cout << srs.wkt() << std::endl;

    std::cout << std::endl;
}

} // namespace app
} // namespace entwine

