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

    m_ap.add(
            "--colorType",
            "The coloring for the output tileset.  May be omitted to choose "
            "default to RGB or Intensity, in that order, if they exist.\n"
            "Valid values:\n"
            "'none': no color\n"
            "'rgb': color by RGB values\n"
            "'intensity': grayscale by intensity values\n"
            "'tile': random color for each tile",
            [this](Json::Value v) { m_json["colorType"] = v.asString(); });

    m_ap.add(
            "--truncate",
            "3D Tiles supports 8-bit color values.  If RGB (or Intensity, if "
            "using intensity colorType) values are 16-bit, set this option to "
            "scale them to 8-bit.",
            [this](Json::Value v)
            {
                checkEmpty(v);
                m_json["truncate"] = true;
            });
}

void Convert::run()
{
    cesium::Tileset tileset(m_json);

    std::cout << "Converting:" << std::endl;
    std::cout << "\tInput:  " << tileset.in().prefixedRoot() << "\n";
    std::cout << "\tOutput: " << tileset.out().prefixedRoot() << "\n";
    std::cout << "\tColor:  " << tileset.colorString() << std::endl;
    std::cout << "\tTruncate: " << (tileset.truncate() ? "yes" : "no") << "\n";
    std::cout << "\tThreads: " << tileset.threadPool().numThreads() << "\n";
    std::cout << "\tRoot geometric error: " <<
        tileset.rootGeometricError() << "\n";

    std::cout << "Running..." << std::endl;
    tileset.build();
    std::cout << "\tDone." << std::endl;
}

} // namespace app
} // namespace entwine

