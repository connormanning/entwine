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

class NewClipper
{
    class Clip
    {
    public:
        Clip(NewClipper& c) : m_clipper(c) { }
        ~Clip() { assert(empty()); }

        bool insert(const Xyz& p)
        {
            const auto it(m_touched.find(p));
            if (it == m_touched.end())
            {
                m_touched[p] = true;
                return true;
            }
            else
            {
                it->second = true;
                return false;
            }
        }

        void clip(const uint64_t d, const bool force)
        {
            for (auto it(m_touched.begin()); it != m_touched.end(); )
            {
                if (force || !it->second)
                {
                    const Xyz& p(it->first);
                    m_clipper.clip(d, p);
                    it = m_touched.erase(it);
                }
                else
                {
                    it->second = false;
                    ++it;
                }
            }
        }

        bool empty() const { return m_touched.empty(); }

    private:
        NewClipper& m_clipper;

        std::map<Xyz, bool> m_touched;
    };

public:
    NewClipper(Registry& registry, Origin origin)
        : m_registry(registry)
        , m_origin(origin)
        , m_clips(64, *this)
    { }

    ~NewClipper() { clip(true); }

    bool insert(uint64_t d, const Xyz& v)
    {
        return m_clips.at(d).insert(v);
    }

    void clip(bool force = false);

    const Origin origin() const { return m_origin; }

private:
    void clip(uint64_t d, const Xyz& p);

    Registry& m_registry;
    const Origin m_origin;

    std::vector<Clip> m_clips;
};


} // namespace entwine

