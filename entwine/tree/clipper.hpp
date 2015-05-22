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
#include <unordered_set>

namespace entwine
{

class Branch;
class Builder;

class Clipper
{
public:
    Clipper(Builder& builder);
    ~Clipper();

    bool insert(std::size_t index);

    void clip();

private:
    Builder& m_builder;
    std::unordered_set<std::size_t> m_clips;
};

} // namespace entwine

