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

#include <map>
#include <set>

#include <entwine/builder/heuristics.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Metadata;

class Hierarchy
{
public:
    using Map = std::map<Dxyz, uint64_t>;

    Hierarchy() { }

    Hierarchy(
            const Metadata& metadata,
            const arbiter::Endpoint& ep,
            bool exists);

    void set(const Dxyz& key, uint64_t val)
    {
        SpinGuard lock(m_spin);
        auto it(m_map.find(key));
        if (it == m_map.end()) m_map[key] = val;
        else it->second = val;
    }

    uint64_t get(const Dxyz& key) const
    {
        SpinGuard lock(m_spin);
        auto it(m_map.find(key));
        if (it == m_map.end()) return 0;
        else return it->second;
    }

    const Map& map() const { return m_map; }

    void save(
            const Metadata& metadata,
            const arbiter::Endpoint& top,
            Pool& pool) const;

    void analyze(const Metadata& m, bool verbose) const;
    void setStep(uint64_t step) const { m_step = step; }

private:
    struct Analysis
    {
        Analysis() { }
        Analysis(const Map& hierarchy, const Map& analyzed, uint64_t step);

        uint64_t step = 0;
        uint64_t totalFiles = 0;
        uint64_t totalNodes = 0;
        uint64_t maxNodesPerFile = 0;
        double mean = 0;
        double stddev = 0;
        double rsd = 0;

        bool fits() const { return maxNodesPerFile <= 65536; }

        void summarize() const;
        bool operator<(const Analysis& b) const;
    };

    using AnalysisSet = std::set<Analysis>;

    std::string filename(const Metadata& m, const Dxyz& dxyz) const
    {
        return dxyz.toString() + m.postfix() + ".json";
    }

    std::string filename(const Metadata& m, const ChunkKey& k) const
    {
        return filename(m, k.dxyz());
    }

    void load(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            const Dxyz& key = Dxyz());

    void save(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            Pool& pool,
            const ChunkKey& key,
            json& j) const;

    void analyze(
            const Metadata& m,
            uint64_t step,
            const ChunkKey& key,
            const Dxyz& curr,
            Map& map) const;

    mutable SpinLock m_spin;
    Map m_map;
    mutable uint64_t m_step = 0;
};

} // namespace entwine

