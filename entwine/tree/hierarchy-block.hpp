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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>

#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/format-types.hpp>
#include <entwine/types/structure.hpp>
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
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints,
            std::size_t size);

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
            const std::vector<char>& data,
            bool readOnly = false);

    void save(const arbiter::Endpoint& ep, std::string pf = "");

    // Only count must be thread-safe.  Get/save are single-threaded.
    virtual HierarchyCell& count(const Id& id, uint64_t tick, int delta) = 0;
    virtual uint64_t get(const Id& id, uint64_t tick) const = 0;

    const Id& id() const { return m_id; }
    const Id& maxPoints() const { return m_maxPoints; }
    std::size_t size() const { return m_size; }

protected:
    Id normalize(const Id& id) const { return id - m_id; }

    void push(std::vector<char>& data, uint64_t val) const
    {
        data.insert(
                data.end(),
                reinterpret_cast<const char*>(&val),
                reinterpret_cast<const char*>(&val) + sizeof(val));
    }

    Id endId() const { return m_id + m_maxPoints; }

    virtual std::vector<char> combine() = 0;

    HierarchyCell::Pool& m_pool;
    const Metadata& m_metadata;
    const Id m_id;
    const arbiter::Endpoint* m_ep;
    const Id m_maxPoints;
    const std::size_t m_size;
};

class ContiguousBlock : public HierarchyBlock
{
    friend class BaseBlock;

public:
    using HierarchyBlock::get;

    ContiguousBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            std::size_t maxPoints)
        : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints, 0)
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

    ContiguousBlock(ContiguousBlock&& other) = default;

    virtual HierarchyCell& count(
            const Id& global,
            uint64_t tick,
            int delta) override
    {
        assert(global >= m_id && global < m_id + m_maxPoints);

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

    void merge(const ContiguousBlock& other)
    {
        for (std::size_t tube(0); tube < other.m_tubes.size(); ++tube)
        {
            for (const auto& cell : other.m_tubes[tube])
            {
                count(other.id() + tube, cell.first, cell.second->val());
            }
        }
    }

    const std::vector<HierarchyTube>& tubes() const { return m_tubes; }

private:
    virtual std::vector<char> combine() override;

    bool empty() const;
    std::vector<HierarchyTube>& tubes() { return m_tubes; }

    std::vector<HierarchyTube> m_tubes;
    std::vector<SpinLock> m_spinners;
};


class BaseBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    BaseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const arbiter::Endpoint* outEndpoint);

    BaseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const arbiter::Endpoint* outEndpoint,
            const std::vector<char>& data);

    virtual HierarchyCell& count(
            const Id& id,
            uint64_t tick,
            int delta) override
    {
        const std::size_t depth(ChunkInfo::calcDepth(id.getSimple()));
        return m_blocks.at(depth).count(id, tick, delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const std::size_t depth(ChunkInfo::calcDepth(id.getSimple()));
        return m_blocks.at(depth).get(id, tick);
    }

    std::set<Id> merge(BaseBlock& other);

    std::vector<ContiguousBlock>& blocks() { return m_blocks; }
    const std::vector<ContiguousBlock>& blocks() const { return m_blocks; }

private:
    virtual std::vector<char> combine() override;

    void makeWritable();

    std::vector<ContiguousBlock> m_blocks;
    std::vector<std::vector<ContiguousBlock>> m_writes;
};

class SparseBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    SparseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints)
        : HierarchyBlock(pool, metadata, id, outEndpoint, maxPoints, 0)
        , m_spinner()
        , m_tubes()
    { }

    SparseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints,
            const std::vector<char>& data);

    virtual HierarchyCell& count(
            const Id& id,
            uint64_t tick,
            int delta) override
    {
        assert(id >= m_id && id < m_id + m_maxPoints);

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

    const std::map<Id, HierarchyTube>& tubes() const { return m_tubes; }

private:
    virtual std::vector<char> combine() override;

    SpinLock m_spinner;
    std::map<Id, HierarchyTube> m_tubes;
};

class ReadOnlySparseBlock : public HierarchyBlock
{
public:
    struct Cell
    {
        Cell(const Id& id, uint64_t tick, uint64_t count = 0)
            : id(id)
            , tick(tick)
            , count(count)
        { }

        Id id;
        uint64_t tick;
        uint64_t count;

        bool operator==(const Cell& other) const
        {
            return id == other.id && tick == other.tick;
        }

        bool operator<(const Cell& other) const
        {
            return id < other.id || (id == other.id && tick < other.tick);
        }
    };

    ReadOnlySparseBlock(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const Id& id,
            const arbiter::Endpoint* outEndpoint,
            const Id& maxPoints,
            const std::vector<char>& data);

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        Cell search(normalize(id), tick);
        const auto lb(std::lower_bound(m_data.begin(), m_data.end(), search));

        if (lb != m_data.end() && *lb == search) return lb->count;
        else return 0;
    }

    virtual HierarchyCell& count(
            const Id& id,
            uint64_t tick,
            int delta) override
    {
        throw std::runtime_error("Cannot count a read-only block");
    }

private:
    virtual std::vector<char> combine() override
    {
        throw std::runtime_error("Cannot combine a read-only block");
    }

    std::vector<Cell> m_data;
};

} // namespace entwine

