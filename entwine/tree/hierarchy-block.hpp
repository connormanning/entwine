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
#include <cstdint>

#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/format-types.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class HierarchyCell
{
public:
    using Pool = splicer::ObjectPool<HierarchyCell>;
    using RawNode = Pool::NodeType;
    using RawStack = Pool::StackType;
    using PooledNode = Pool::UniqueNodeType;
    using PooledStack = Pool::UniqueStackType;

    HierarchyCell() : m_val(0), m_spinner() { }
    HierarchyCell(uint64_t val) : m_val(val), m_spinner() { }

    HierarchyCell& operator=(const HierarchyCell& other)
    {
        m_val = other.val();
        return *this;
    }

    HierarchyCell& count(int delta)
    {
        SpinGuard lock(m_spinner);
        m_val = static_cast<int>(m_val) + delta;
        return *this;
    }

    uint64_t val() const { return m_val; }

private:
    uint64_t m_val;
    SpinLock m_spinner;
};

using HierarchyTube = std::map<uint64_t, HierarchyCell::PooledNode>;

namespace arbiter { class Endpoint; }

class Metadata;

class HierarchyBlock
{
public:
    static std::size_t count();

    HierarchyBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint);

    virtual ~HierarchyBlock();

    static std::unique_ptr<HierarchyBlock> create(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints);

    static std::unique_ptr<HierarchyBlock> create(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints,
            const std::vector<char>& data);

    void save(const arbiter::Endpoint& ep, std::string pf = "");

    // Only count must be thread-safe.  Get/save are single-threaded.
    virtual HierarchyCell& count(const Id& id, uint64_t tick, int delta) = 0;
    virtual uint64_t get(const Id& id, uint64_t tick) const = 0;

protected:
    Id normalize(const Id& id) const { return id - m_id; }

    void push(std::vector<char>& data, uint64_t val) const
    {
        data.insert(
                data.end(),
                reinterpret_cast<const char*>(&val),
                reinterpret_cast<const char*>(&val) + sizeof(val));
    }

    virtual std::vector<char> combine() const = 0;

    HierarchyCell::Pool& m_pool;
    const Metadata& m_metadata;
    const Id m_id;
    const arbiter::Endpoint* m_ep;
};

class ContiguousBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    ContiguousBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            std::size_t maxPoints)
        : HierarchyBlock(pool, metadata, id, outEndpoint)
        , m_tubes(maxPoints)
        , m_spinners(maxPoints)
    { }

    ContiguousBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            std::size_t maxPoints,
            const std::vector<char>& data);

    virtual HierarchyCell& count(
            const Id& global,
            uint64_t tick,
            int delta) override
    {
        const std::size_t id(normalize(global).getSimple());

        SpinGuard lock(m_spinners.at(id));
        auto& tube(m_tubes.at(id));
        auto it(tube.find(tick));
        if (it == tube.end())
        {
            it = tube.insert(std::make_pair(tick, m_pool.acquireOne())).first;
        }
        return it->second->count(delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const HierarchyTube& tube(m_tubes.at(normalize(id).getSimple()));
        const auto it(tube.find(tick));
        if (it != tube.end()) return it->second->val();
        else return 0;
    }

    void merge(const ContiguousBlock& other);

private:
    virtual std::vector<char> combine() const override;

    std::vector<HierarchyTube> m_tubes;
    std::vector<SpinLock> m_spinners;
};

class SparseBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    SparseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint)
        : HierarchyBlock(pool, metadata, id, outEndpoint)
        , m_spinner()
        , m_tubes()
    { }

    SparseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const std::vector<char>& data);

    virtual HierarchyCell& count(
            const Id& id,
            uint64_t tick,
            int delta) override
    {
        SpinGuard lock(m_spinner);
        auto& tube(m_tubes[normalize(id)]);
        auto it(tube.find(tick));
        if (it == tube.end())
        {
            it = tube.insert(std::make_pair(tick, m_pool.acquireOne())).first;
        }
        return it->second->count(delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const auto tubeIt(m_tubes.find(normalize(id)));
        if (tubeIt != m_tubes.end())
        {
            const HierarchyTube& tube(tubeIt->second);
            const auto cellIt(tube.find(tick));
            if (cellIt != tube.end())
            {
                return cellIt->second->val();
            }
        }

        return 0;
    }

private:
    virtual std::vector<char> combine() const override;

    SpinLock m_spinner;
    std::map<Id, HierarchyTube> m_tubes;
};

} // namespace entwine

