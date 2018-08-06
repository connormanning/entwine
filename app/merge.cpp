/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "merge.hpp"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <json/json.h>

#include <entwine/builder/config.hpp>
#include <entwine/builder/merger.hpp>

namespace entwine
{
namespace app
{

void Merge::addArgs()
{
    m_ap.setUsage("entwine merge <path> (<options>)");

    addOutput("Path containing completed subset builds");
    addConfig();
    addSimpleThreads();
    addArbiter();
}

void Merge::run()
{
    m_json["verbose"] = true;
    Config config(m_json);
    Merger merger(config);
    std::cout << "Merging " << config.output() << "..." << std::endl;
    merger.go();
    std::cout << "Merge complete." << std::endl;
}

} // namespace app
} // namespace entwine

