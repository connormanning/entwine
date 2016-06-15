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

#include <memory>
#include <mutex>

// #include <entwine/tree/climber.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

template<typename T>
class Splitter
{
protected:
    using UniqueT = std::unique_ptr<T>;

    struct Slot
    {
        Slot() : mark(false), spinner(), t() { }

        bool mark;  // Data exists?
        SpinLock spinner;
        UniqueT t;
    };

public:
    Splitter(const Structure& structure)
        : m_structure(structure)
        , m_base()
        , m_fast(getNumFastTrackers(structure))
        , m_slow()
        , m_slowMutex()
    { }

    virtual ~Splitter() { }

    void mark(const Id& chunkId, std::size_t chunkNum)
    {
        get(chunkId, chunkNum).mark = true;
    }

    UniqueT& base() { return &m_base; }

    Slot& get(const PointState& pointState)
    {
        assert(!m_structure.isWithinBase(pointState.depth()));
        return get(pointState.chunkId(), pointState.chunkNum());
    }

    Slot& get(const Id& chunkId, std::size_t chunkNum)
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

    std::set<Id> ids() const
    {
        std::set<Id> results;

        for (std::size_t i(0); i < m_fast.size(); ++i)
        {
            if (m_fast[i].mark)
            {
                results.insert(m_structure.getInfoFromNum(i).chunkId());
            }
        }

        for (const auto& p : m_slow)
        {
            results.insert(p.first);
        }

        return results;
    }

protected:
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

    UniqueT m_base;
    std::vector<Slot> m_fast;
    std::map<Id, Slot> m_slow;

    std::mutex m_slowMutex;
};


} // namespace entwine

