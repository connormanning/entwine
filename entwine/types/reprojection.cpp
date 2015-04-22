/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/reprojection.hpp>

namespace entwine
{

Reprojection::Reprojection()
    : m_in()
    , m_out()
    , m_valid(false)
{ }

Reprojection::Reprojection(const std::string in, const std::string out)
    : m_in(in)
    , m_out(out)
    , m_valid(true)
{ }

std::string Reprojection::in() const
{
    return m_in;
}

std::string Reprojection::out() const
{
    return m_out;
}

bool Reprojection::valid() const
{
    return m_valid;
}

} // namespace entwine

