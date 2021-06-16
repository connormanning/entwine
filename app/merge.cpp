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
#include <mutex>
#include <string>

#include <entwine/builder/builder.hpp>
#include <entwine/types/endpoints.hpp>
#include <entwine/util/config.hpp>

namespace entwine
{
namespace app
{

void Merge::addArgs()
{
    m_ap.setUsage("entwine merge <path> (<options>)");

    addOutput("Path containing completed subset builds", true);
    addConfig();
    addTmp();
    addSimpleThreads();
    addArbiter();
    m_ap.add(
            "--force",
            "-f",
            "Force merge overwrite - if a completed EPT dataset exists at this "
            "output location, overwrite it with the result of the merge.",
            [this](json j) { checkEmpty(j); m_json["force"] = true; });
}

void Merge::run()
{
    builder::merge(m_json);
}

} // namespace app
} // namespace entwine
