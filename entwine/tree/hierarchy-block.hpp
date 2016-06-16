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

#include <entwine/types/defs.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class HierarchyCell
{
public:
    HierarchyCell() : m_val(0), m_spinner() { }
    HierarchyCell(uint64_t val) : m_val(val), m_spinner() { }

    HierarchyCell& operator=(const HierarchyCell& other)
    {
        m_val = other.val();
        return *this;
    }

    void count(int delta)
    {
        SpinGuard lock(m_spinner);
        m_val = static_cast<int>(m_val) + delta;
    }

    uint64_t val() const { return m_val; }

private:
    uint64_t m_val;
    SpinLock m_spinner;
};

using HierarchyTube = std::map<uint64_t, HierarchyCell>;

class Structure;

class HierarchyBlock
{
public:
    HierarchyBlock(const Id& id) : m_id(id) { }

    static std::unique_ptr<HierarchyBlock> create(
            const Structure& structure,
            const Id& id,
            const Id& maxPoints);

    static std::unique_ptr<HierarchyBlock> create(
            const Structure& structure,
            const Id& id,
            const Id& maxPoints,
            const std::vector<char>& data);

    // Only count must be thread-safe.  Get/save are single-threaded.
    virtual void count(const Id& id, uint64_t tick, int delta) = 0;

    virtual uint64_t get(const Id& id, uint64_t tick) const = 0;
    virtual void save(const arbiter::Endpoint& ep, std::string pf = "") = 0;

protected:
    Id normalize(const Id& id) const { return id - m_id; }
    void push(std::vector<char>& data, uint64_t val)
    {
        data.insert(
                data.end(),
                reinterpret_cast<const char*>(&val),
                reinterpret_cast<const char*>(&val) + sizeof(val));
    }

    const Id m_id;
};

class ContiguousBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    ContiguousBlock(const Id& id, std::size_t maxPoints)
        : HierarchyBlock(id)
        , m_tubes(maxPoints)
    { }

    ContiguousBlock(
            const Id& id,
            std::size_t maxPoints,
            const std::vector<char>& data);

    virtual void count(const Id& id, uint64_t tick, int delta) override
    {
        m_tubes.at(normalize(id).getSimple())[tick].count(delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const HierarchyTube& tube(m_tubes.at(normalize(id).getSimple()));
        const auto it(tube.find(tick));
        if (it != tube.end()) return it->second.val();
        else return 0;
    }

    virtual void save(
            const arbiter::Endpoint& ep,
            std::string pf = "") override;

    void merge(const ContiguousBlock& other);

private:
    std::vector<HierarchyTube> m_tubes;
};

class SparseBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    SparseBlock(const Id& id) : HierarchyBlock(id), m_spinner(), m_tubes() { }
    SparseBlock(const Id& id, const std::vector<char>& data);

    virtual void count(const Id& id, uint64_t tick, int delta) override
    {
        SpinGuard lock(m_spinner);
        m_tubes[normalize(id)][tick].count(delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const auto tubeIt(m_tubes.find(id));
        if (tubeIt != m_tubes.end())
        {
            const HierarchyTube& tube(tubeIt->second);
            const auto cellIt(tube.find(tick));
            if (cellIt != tube.end())
            {
                return cellIt->second.val();
            }
        }

        return 0;
    }

    virtual void save(
            const arbiter::Endpoint& ep,
            std::string pf = "") override;

private:
    SpinLock m_spinner;
    std::map<Id, HierarchyTube> m_tubes;
};

} // namespace entwine

