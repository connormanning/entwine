/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cassert>
#include <cstdint>
#include <set>

#include <entwine/types/defs.hpp>
#include <entwine/types/key.hpp>

namespace entwine
{

class Registry;
class ReffedChunk;

class Clipper
{
    class Clip
    {
    public:
        Clip(Clipper& c) : m_clipper(c) { }
        ~Clip() { assert(empty()); }

        bool insert(ReffedChunk& c);
        std::size_t clip(bool force = false);
        bool empty() const { return m_chunks.empty(); }

    private:
        Clipper& m_clipper;

        struct Cmp
        {
            bool operator()(const ReffedChunk* a, const ReffedChunk* b) const;
        };

        std::map<ReffedChunk*, bool, Cmp> m_chunks;
    };

public:
    Clipper(Registry& registry, Origin origin = 0)
        : m_registry(registry)
        , m_origin(origin)
        , m_clips(64, *this)
    { }

    ~Clipper() { if (m_origin != invalidOrigin) clipAll(); }

    Registry& registry() { return m_registry; }

    bool insert(ReffedChunk& c);

    void clip();

    const Origin origin() const { return m_origin; }

private:
    void clipAll();

    Registry& m_registry;
    const Origin m_origin;

    std::size_t m_count = 0;
    std::vector<Clip> m_clips;
};

} // namespace entwine

