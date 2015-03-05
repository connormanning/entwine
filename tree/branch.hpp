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

#include <cstdint>

#include "types/schema.hpp"

namespace Json
{
    class Value;
}

struct Point;
struct PointInfo;
class Roller;

class Branch
{
public:
    Branch(const Schema& schema, std::size_t begin, std::size_t end)
        : m_schema(schema)
        , m_begin(begin)
        , m_end(end)
    { }

    virtual ~Branch() { }

    // Returns true if this point was successfully stored.
    virtual bool putPoint(PointInfo** toAddPtr, const Roller& roller) = 0;

    // Null pointer indicates that there is no point at this index.
    virtual const Point* getPoint(std::size_t index) const = 0;

    virtual void save(const std::string& dir, Json::Value& meta) const = 0;
    virtual void load(const std::string& dir, const Json::Value& meta) = 0;

protected:
    const Schema& schema() const { return m_schema; }
    std::size_t begin() const { return m_begin; }
    std::size_t end()   const { return m_end; }
    std::size_t size()  const { return m_end - m_begin; }

private:
    const Schema& m_schema;
    const std::size_t m_begin;
    const std::size_t m_end;
};

