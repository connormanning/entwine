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
#include <entwine/types/format-types.hpp>
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

class Metadata;

class HierarchyBlock
{
public:
    HierarchyBlock(const Id& id, HierarchyCompression c)
        : m_id(id)
        , m_compress(c)
    { }

    static std::unique_ptr<HierarchyBlock> create(
            const Metadata& metadata,
            const Id& id,
            const Id& maxPoints);

    static std::unique_ptr<HierarchyBlock> create(
            const Metadata& metadata,
            const Id& id,
            const Id& maxPoints,
            const std::vector<char>& data);

    void save(const arbiter::Endpoint& ep, std::string pf = "");

    // Only count must be thread-safe.  Get/save are single-threaded.
    virtual void count(const Id& id, uint64_t tick, int delta) = 0;
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

    const Id m_id;

private:
    const HierarchyCompression m_compress;
};

class ContiguousBlock : public HierarchyBlock
{
public:
    using HierarchyBlock::get;

    ContiguousBlock(const Id& id, HierarchyCompression c, std::size_t maxPoints)
        : HierarchyBlock(id, c)
        , m_tubes(maxPoints)
        , m_spinners(maxPoints)
    { }

    ContiguousBlock(
            const Id& id,
            HierarchyCompression c,
            std::size_t maxPoints,
            const std::vector<char>& data);

    virtual void count(const Id& global, uint64_t tick, int delta) override
    {
        const std::size_t id(normalize(global).getSimple());

        SpinGuard lock(m_spinners.at(id));
        m_tubes.at(id)[tick].count(delta);
    }

    virtual uint64_t get(const Id& id, uint64_t tick) const override
    {
        const HierarchyTube& tube(m_tubes.at(normalize(id).getSimple()));
        const auto it(tube.find(tick));
        if (it != tube.end()) return it->second.val();
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

    SparseBlock(const Id& id, HierarchyCompression c)
        : HierarchyBlock(id, c)
        , m_spinner()
        , m_tubes()
    { }

    SparseBlock(
            const Id& id,
            HierarchyCompression c,
            const std::vector<char>& data);

    virtual void count(const Id& id, uint64_t tick, int delta) override
    {
        SpinGuard lock(m_spinner);
        m_tubes[normalize(id)][tick].count(delta);
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
                return cellIt->second.val();
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

