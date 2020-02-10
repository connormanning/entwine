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

#include <cstdint>
#include <map>

#include <entwine/types/key.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

struct Hierarchy
{
    using Map = std::map<Dxyz, int64_t>;
    using ChunkMap = std::map<Dxyz, Hierarchy::Map>;

    Hierarchy() = default;
    Hierarchy(const Hierarchy& other) : map(other.map) { }
    Hierarchy& operator=(const Hierarchy& other)
    {
        map = other.map;
        return *this;
    }

    mutable SpinLock spin;
    Map map = { { Dxyz(), 0 } };
};

void to_json(json& j, const Hierarchy& h);
void from_json(const json& j, Hierarchy& h);

namespace hierarchy
{

inline void set(Hierarchy& h, const Dxyz& key, uint64_t val)
{
    SpinGuard lock(h.spin);
    h.map[key] = val;
}

inline uint64_t get(const Hierarchy& h, const Dxyz& key)
{
    SpinGuard lock(h.spin);
    const auto it = h.map.find(key);
    if (it == h.map.end()) return 0;
    else return it->second;
}

unsigned determineStep(const Hierarchy& h);
Hierarchy::ChunkMap getChunks(const Hierarchy& h, unsigned step = 0);
void save(
    const Hierarchy& h,
    const arbiter::Endpoint& ep,
    unsigned step,
    unsigned threads,
    std::string postfix = "");
Hierarchy load(
    const arbiter::Endpoint& ep,
    unsigned threads,
    std::string postfix = "");

} // namespace hierarchy
} // namespace entwine
