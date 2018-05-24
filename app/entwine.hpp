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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

class App
{
public:
    static void build(std::vector<std::string> args);
    static void merge(std::vector<std::string> args);
    static void infer(std::vector<std::string> args);
    static void rebase(std::vector<std::string> args);

private:
    static std::string yesNo(bool b) { return b ? "yes" : "no"; }

    static std::string getDimensionString(const Schema& schema)
    {
        const DimList dims(schema.dims());
        std::string results("[\n");
        const std::string prefix(16, ' ');
        const std::size_t width(80);

        std::string line;

        for (std::size_t i(0); i < dims.size(); ++i)
        {
            const auto name(dims[i].name());
            const bool last(i == dims.size() - 1);

            if (prefix.size() + line.size() + name.size() + 1 >= width)
            {
                results += prefix + line + '\n';
                line.clear();
            }

            if (line.size()) line += ' ';
            line += dims[i].name();

            if (!last) line += ',';
            else results += prefix + line + '\n';
        }

        results += "\t]";

        return results;
    }
};

} // namespace entwine

