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

        bool insert(uint64_t x, uint64_t y, uint64_t z)
        {
            const auto a(m_touched.find(x));
            if (a == m_touched.end())
            {
                m_touched[x][y][z] = true;
                return true;
            }

            auto& av(a->second);
            auto b(av.find(y));
            if (b == av.end())
            {
                av[y][z] = true;
                return true;
            }

            auto& bv(b->second);
            auto c(bv.find(z));
            if (c == bv.end())
            {
                bv[z] = true;
                return true;
            }

            c->second = true;
            return false;
        }

        void clip(const uint64_t d, const bool force)
        {
            for (auto a(m_touched.begin()); a != m_touched.end(); )
            {
                auto& av(a->second);
                for (auto b(av.begin()); b != av.end(); )
                {
                    auto& bv(b->second);
                    for (auto c(bv.begin()); c != bv.end(); )
                    {
                        auto& cv(c->second);
                        if (force || !cv)
                        {
                            m_clipper.clip(d, a->first, b->first, c->first);
                            c = bv.erase(c);
                        }
                        else
                        {
                            cv = false;
                            ++c;
                        }
                    }

                    if (bv.empty()) b = av.erase(b);
                    else ++b;
                }

                if (av.empty()) a = m_touched.erase(a);
                else ++a;
            }
        }

        bool empty() const { return m_touched.empty(); }

    private:
        NewClipper& m_clipper;

        // TODO This needs to be abstracted away, as well as the repetitive
        // code throughout this class.
        std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, bool>>>
            m_touched;
    };

public:
    NewClipper(Registry& registry, Origin origin)
        : m_registry(registry)
        , m_origin(origin)
        , m_clips(64, *this)
    { }

    ~NewClipper() { clip(true); }

    bool insert(uint64_t d, uint64_t x, uint64_t y, uint64_t z)
    {
        return m_clips.at(d).insert(x, y, z);
    }

    void clip(bool force = false);

    const Origin origin() const { return m_origin; }

private:
    void clip(uint64_t d, uint64_t x, uint64_t y, uint64_t z);

    Registry& m_registry;
    const Origin m_origin;

    std::vector<Clip> m_clips;
};


} // namespace entwine

