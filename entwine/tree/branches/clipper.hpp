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
#include <set>

namespace entwine
{

class Branch;
class SleepyTree;

class Clipper
{
public:
    Clipper(SleepyTree& tree);
    ~Clipper();

    bool insert(std::size_t index);

    void clip();

private:
    SleepyTree& m_tree;
    std::set<std::size_t> m_clips;
};

} // namespace entwine

