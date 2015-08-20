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

#include <string>

#include <entwine/third/json/json.hpp>

namespace entwine
{

class Reprojection
{
public:
    Reprojection(std::string in, std::string out);
    Reprojection(const Json::Value& json);

    Json::Value toJson() const;

    std::string in() const;
    std::string out() const;

private:
    std::string m_in;
    std::string m_out;
};

} // namespace entwine

