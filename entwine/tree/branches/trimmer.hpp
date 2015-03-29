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

#include <cstddef>
#include <map>

namespace entwine
{

class Branch;

class Trimmer
{
public:
    Trimmer();
    ~Trimmer();

    void trim();

private:
    std::map<Branch*, std::size_t> m_clips;
};

} // namespace entwine

