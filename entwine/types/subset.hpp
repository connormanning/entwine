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
#include <memory>
#include <vector>

#include <entwine/types/bounds.hpp>

namespace Json
{
    class Value;
}

namespace entwine
{

class Metadata;
class Structure;

class Subset
{
public:
    Subset(const Bounds& boundsNativeCubic, std::size_t id, std::size_t of);
    Subset(const Bounds& boundsNativeCubic, const Json::Value& json);

    Json::Value toJson() const;

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }
    const Bounds& bounds() const { return m_sub; }

    std::string postfix() const { return "-" + std::to_string(m_id); }
    bool primary() const { return !m_id; }

    // The minimum null depth represents the depth at which *nodes* first
    // diverge completely.
    std::size_t minimumNullDepth() const { return m_minimumNullDepth; }

    // The minimum base depth represents the depth at which *chunks* first
    // diverge completely.
    std::size_t minimumBaseDepth(std::size_t pointsPerChunk) const;

    class Span
    {
    public:
        Span(std::size_t begin, std::size_t end)
            : m_begin(begin)
            , m_end(end)
        { }

        Span() : m_begin(0), m_end(0) { }

        bool operator<(const Span& other) const
        {
            return m_begin < other.m_begin;
        }

        void merge(const Span& other)
        {
            if (end() != other.begin())
            {
                throw std::runtime_error("Cannot merge these spans");
            }

            m_end = other.end();
        }

        void up()
        {
            m_begin >>= 2;
            m_end >>= 2;
        }

        std::size_t begin() const { return m_begin; }
        std::size_t end() const { return m_end; }

    private:
        std::size_t m_begin;
        std::size_t m_end;
    };

    std::vector<Span> calcSpans(
            const Structure& structure,
            const Bounds& bounds) const;

private:
    std::size_t m_id;
    std::size_t m_of;

    Bounds m_sub;
    std::size_t m_minimumNullDepth;

    std::vector<Bounds> m_boxes;
};

} // namespace entwine

