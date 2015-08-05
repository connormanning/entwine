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

#include <entwine/types/structure.hpp>

namespace entwine
{

class Branch;
class Builder;

class Clipper
{
public:
    Clipper(Builder& builder);
    ~Clipper();

    bool insert(const Id& index);

    void clip();

private:
    Builder& m_builder;
    std::unordered_set<Id> m_clips;
};

} // namespace entwine

