/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include <json/json.h>

#include "arg-parser.hpp"

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{
namespace app
{

class App
{
public:
    virtual ~App() { }

    void go(Args args)
    {
        addArgs();
        if (!m_ap.handle(args)) return;
        run();
    }

protected:
    virtual void addArgs() = 0;
    virtual void run() = 0;

    Json::Value m_json;
    ArgParser m_ap;

    void addInput(std::string description, bool asDefault = false);
    void addOutput(std::string description, bool asDefault = false);
    void addConfig();
    void addTmp();
    void addSimpleThreads();
    void addReprojection();
    void addNoTrustHeaders();
    void addAbsolute();
    void addArbiter();

    void checkEmpty(Json::Value v) const
    {
        if (!v.isNull()) throw std::runtime_error("Invalid specification");
    }

    Json::UInt64 extract(Json::Value v) const
    {
        return parse(v.asString()).asUInt64();
    }

    std::string yesNo(bool b) const { return b ? "yes" : "no"; }

    std::string getDimensionString(const Schema& schema) const;
};

} // namespace app
} // namespace entwine

