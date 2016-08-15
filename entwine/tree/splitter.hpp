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
#include <cassert>
#include <memory>
#include <mutex>

#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

template<typename T>
class Splitter
{
protected:
    using UniqueT = std::unique_ptr<T>;

public:
    struct Slot
    {
        Slot() : mark(false), spinner(), t() { }

        bool mark;  // Data exists?
        mutable SpinLock spinner;
        mutable UniqueT t;
    };

    using SlotType = Splitter<T>::Slot;

    Splitter(const Structure& structure)
        : m_structure(structure)
        , m_base()
        , m_fast(getNumFastTrackers(structure))
        , m_slow()
        , m_faux()
        , m_slowMutex()
    { }

    virtual ~Splitter() { }

    void mark(const Id& chunkId, std::size_t chunkNum)
    {
        getOrCreate(chunkId, chunkNum).mark = true;
    }

    bool isWithinBase(std::size_t depth) const
    {
        return m_structure.isWithinBase(depth);
    }

    Slot& getOrCreate(const Id& chunkId, std::size_t chunkNum)
    {
        if (chunkNum < m_fast.size())
        {
            return m_fast[chunkNum];
        }
        else
        {
            std::lock_guard<std::mutex> lock(m_slowMutex);
            return m_slow[chunkId];
        }
    }

    Slot& at(const Id& chunkId, std::size_t chunkNum)
    {
        if (chunkNum < m_fast.size())
        {
            return m_fast[chunkNum];
        }
        else
        {
            std::lock_guard<std::mutex> lock(m_slowMutex);
            return m_slow.at(chunkId);
        }
    }

    // As opposed to getOrCreate(), this operation will search the base Slot.
    const Slot* tryGet(
            const Id& chunkId,
            std::size_t chunkNum,
            std::size_t depth) const
    {
        const Slot* slot(nullptr);
        if (isWithinBase(depth)) slot = &m_base;
        else if (chunkNum < m_fast.size()) slot = &m_fast[chunkNum];
        else if (m_slow.count(chunkId)) slot = &m_slow.at(chunkId);
        return slot;
    }

    std::set<Id> ids() const
    {
        std::set<Id> results(m_faux);

        iterateCold([&results](const Id& id, std::size_t num, const Slot& slot)
        {
            results.insert(id);
        });

        return results;
    }

    template<typename Op>
    void iterateCold(Op op, Pool* pool = nullptr) const
    {
        pool = nullptr;
        auto call([&op, pool](const Id& id, std::size_t n, const Slot& slot)
        {
            if (pool) pool->add([&op, id, n, &slot]() { op(id, n, slot); });
            else op(id, n, slot);
        });

        for (std::size_t i(0); i < m_fast.size(); ++i)
        {
            if (m_fast[i].mark)
            {
                call(m_structure.getInfoFromNum(i).chunkId(), i, m_fast[i]);
            }
        }

        for (const auto& p : m_slow) call(p.first, m_fast.size(), p.second);

        if (pool) pool->cycle();
    }

    Slot& base() { return m_base; }
    const Slot& base() const { return m_base; }

protected:
    void merge(const std::set<Id>& s)
    {
        assert(std::none_of(s.begin(), s.end(), [this](const Id& id)
        {
            return ids().count(id);
        }));

        m_faux.insert(s.begin(), s.end());
    }

    std::size_t getNumFastTrackers(const Structure& structure)
    {
        std::size_t count(0);
        std::size_t depth(structure.coldDepthBegin());
        static const std::size_t maxFastTrackers(std::pow(4, 12));

        while (
                count < maxFastTrackers &&
                depth < 64 &&
                (depth < structure.coldDepthEnd() || !structure.coldDepthEnd()))
        {
            count += structure.numChunksAtDepth(depth);
            ++depth;
        }

        return count;
    }

    const Structure& m_structure;

    Slot m_base;
    std::vector<Slot> m_fast;
    std::map<Id, Slot> m_slow;
    std::set<Id> m_faux;

    std::mutex m_slowMutex;
};

} // namespace entwine

