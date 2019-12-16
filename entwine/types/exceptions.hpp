/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <stdexcept>

namespace entwine
{

class FatalError : public std::runtime_error
{
public:
    FatalError(const std::string& s) : std::runtime_error(s) { }
};

} // namespace entwine
