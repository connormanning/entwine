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

#include <entwine/tree/climber.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

template<typename T>
class Splitter
{
public:
    Splitter(const Structure& structure)
        : m_structure(structure)
        , m_base

    T& operator[](const PointState& pointState)
    {
    }

private:
    struct FastSlot
    {
        FastSlot() : mark(false), spinner(), chunk() { }

        std::atomic_bool mark;  // Data exists?
        SpinLock spinner;
        std::unique_ptr<CountedChunk> chunk;
    };

    const Structure& m_structure;

    std::unique_ptr<T> m_base;
    std::vector<FastSlot> m_fast;
    std::map<Id, std::unique_ptr<T>> m_slow;
};


} // namespace entwine

