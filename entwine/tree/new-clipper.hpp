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

namespace entwine
{

struct Position
{
    Position() = default;

    // TODO Remove Z default param.
    Position(uint64_t d, uint64_t x, uint64_t y, uint64_t z = 0)
        : d(d), x(x), y(y), z(z)
    { }

    uint64_t d = 0;
    uint64_t x = 0;
    uint64_t y = 0;
    uint64_t z = 0;
};

inline bool operator<(const Position& a, const Position& b)
{
    if (a.d < b.d) return true;
    if (a.d == b.d)
    {
        if (a.x < b.x) return true;
        else if (a.x == b.x)
        {
            if (a.y < b.y) return true;
            else if (a.y == b.y)
            {
                if (a.z < b.z) return true;
            }
        }
    }
    return false;
}

class Registry;

class NewClipper
{
    class Clip
    {
    public:
        Clip(NewClipper& c) : m_clipper(c) { }
        ~Clip() { assert(empty()); }

        bool insert(uint64_t x, uint64_t y)
        {
            const auto a(m_touched.find(x));
            if (a == m_touched.end())
            {
                m_touched[x][y] = true;
                return true;
            }

            auto& inner(a->second);
            auto b(inner.find(y));
            if (b == inner.end())
            {
                inner.emplace(y, true);
                return true;
            }

            b->second = true;
            return false;
        }

        void clip(const uint64_t d, const bool force)
        {
            for (auto a(m_touched.begin()); a != m_touched.end(); )
            {
                auto& inner(a->second);
                for (auto b(inner.begin()); b != inner.end(); )
                {
                    if (force || !b->second)
                    {
                        m_clipper.clip(d, a->first, b->first);
                        b = inner.erase(b);
                    }
                    else
                    {
                        b->second = false;
                        ++b;
                    }
                }

                if (inner.empty()) a = m_touched.erase(a);
                else ++a;
            }
        }

        bool empty() const { return m_touched.empty(); }

    private:
        NewClipper& m_clipper;
        std::map<uint64_t, std::map<uint64_t, bool>> m_touched;
    };

public:
    NewClipper(Registry& registry, Origin origin)
        : m_registry(registry)
        , m_origin(origin)
        , m_clips(64, *this)
    { }

    ~NewClipper() { clip(true); }

    bool insert(uint64_t d, uint64_t x, uint64_t y)
    {
        return m_clips.at(d).insert(x, y);
    }

    void clip(bool force = false);

    const Origin origin() const { return m_origin; }

private:
    void clip(uint64_t d, uint64_t x, uint64_t y);

    Registry& m_registry;
    const Origin m_origin;

    std::vector<Clip> m_clips;
};


} // namespace entwine

