/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "convert.hpp"

#include <entwine/formats/cesium/tileset.hpp>

namespace entwine
{
namespace app
{

void Convert::addArgs()
{
    m_ap.setUsage("entwine convert <options>");

    m_ap.add(
            "--input",
            "-i",
            "Path to a completed entwine build",
            [this](Json::Value v) { m_json["input"] = v.asString(); });

    addOutput("Path for Cesium 3D Tiles output");
    addTmp();
    addSimpleThreads();

    m_ap.add(
            "--geometricErrorDivisor",
            "-g",
            "The root geometric error is determined as the width of the "
            "dataset cube divided by \"geometricErrorDivisor\", which defaults "
            "to 32.  Smaller values will result in the data being loaded "
            "at higher density\n"
            "Example: --geometricErrorDivisor 16.0",
            [this](Json::Value v)
            {
                m_json["geometricErrorDivisor"] = v.asDouble();
            });
}

void Convert::run()
{
    cesium::Tileset tileset(m_json);

    std::cout << "Converting:" << std::endl;
    std::cout << "\tInput:  " << tileset.in().prefixedRoot() << "\n";
    std::cout << "\tOutput: " << tileset.out().prefixedRoot() << "\n";
    std::cout << "\tTmp:    " << tileset.tmp().prefixedRoot() << "\n";
    std::cout << "\tThreads: " << tileset.threadPool().numThreads() << "\n";

    std::cout << "Running..." << std::endl;
    tileset.build();
    std::cout << "\tDone." << std::endl;
}

} // namespace app
} // namespace entwine

