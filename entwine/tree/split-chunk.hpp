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

#include <entwine/tree/new-chunk.hpp>
#include <entwine/tree/new-climber.hpp>

namespace entwine
{

class Slice;

class ReffedChunk
{
public:
    void ref(const Slice& s, const NewClimber& climber);
    void unref(const Slice& s, const Xyz& p, uint64_t o);

    NewChunk& chunk() { return *m_chunk; }
    std::size_t np() const { return m_np; }
    void setNp(uint64_t np) { m_np = np; }

private:
    std::mutex m_mutex;
    std::size_t m_np = 0;
    std::unique_ptr<NewChunk> m_chunk;
    std::map<Origin, std::size_t> m_refs;
};

class SplitChunk
{
public:
    virtual ~SplitChunk() { }

    static std::unique_ptr<SplitChunk> create(
            bool contiguous,
            std::size_t splits);

    virtual ReffedChunk& get(uint64_t z) = 0;
    virtual ReffedChunk& at(uint64_t z) = 0;
    virtual std::size_t np(uint64_t z) const = 0;
    virtual void setNp(uint64_t z, uint64_t np) = 0;
};

class ContiguousSplitChunk : public SplitChunk
{
public:
    ContiguousSplitChunk(std::size_t n) : m_chunks(n) { }

    ReffedChunk& get(uint64_t z) override
    {
        return m_chunks[z];
    }

    ReffedChunk& at(uint64_t z) override
    {
        return m_chunks.at(z);
    }

    std::size_t np(uint64_t z) const override
    {
        return m_chunks[z].np();
    }

    void setNp(uint64_t z, uint64_t np) override
    {
        m_chunks[z].setNp(np);
    }

private:
    std::vector<ReffedChunk> m_chunks;
};

class MappedSplitChunk : public SplitChunk
{
public:
    ReffedChunk& get(uint64_t z) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_chunks[z];
    }

    ReffedChunk& at(uint64_t z) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_chunks.at(z);
    }

    std::size_t np(uint64_t z) const override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it(m_chunks.find(z));
        if (it != m_chunks.end()) return it->second.np();
        else return 0;
    }

    void setNp(uint64_t z, uint64_t np) override
    {
        get(z).setNp(np);
    }

private:
    mutable std::mutex m_mutex;
    std::map<uint64_t, ReffedChunk> m_chunks;
};

} // namespace entwine

