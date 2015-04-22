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

namespace entwine
{

class Reprojection
{
public:
    Reprojection();
    Reprojection(std::string in, std::string out);

    std::string in() const;
    std::string out() const;
    bool valid() const;

private:
    std::string m_in;
    std::string m_out;
    bool m_valid;
};

} // namespace entwine

