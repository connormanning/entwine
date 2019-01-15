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

#include <string>

#include <entwine/reader/comparison.hpp>
#include <entwine/reader/logic-gate.hpp>
#include <entwine/reader/query-params.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Filter
{
public:
    Filter(const Metadata& m, const QueryParams& p)
        : Filter(m, p.bounds(), p.filter())
    { }

    Filter(
            const Metadata& metadata,
            const Bounds& queryBounds,
            const Json::Value& json) = delete;

    Filter(
            const Metadata& metadata,
            const Bounds& queryBounds,
            const json& j)
        : m_metadata(metadata)
        , m_queryBounds(queryBounds)
        , m_root()
    {
        if (j.is_object()) build(m_root, j);
        else if (!j.is_null()) throw std::runtime_error("Invalid filter type");
    }

    bool check(const pdal::PointRef& pointRef) const
    {
        return m_root.check(pointRef);
    }

    bool check(const Bounds& bounds) const
    {
        return m_queryBounds.overlaps(bounds) && m_root.check(bounds);
    }

    void log() const
    {
        m_root.log("");
    }

private:
    void build(LogicGate& gate, const json& j)
    {
        if (j.is_array())
        {
            for (const json& val : j) build(gate, val);
            return;
        }

        if (!j.is_object())
        {
            throw std::runtime_error("Unexpected filter type: " + j.dump(2));
        }

        LogicGate* active(&gate);

        std::unique_ptr<LogicGate> outer;

        if (j.size() > 1)
        {
            outer = LogicGate::create(LogicalOperator::lAnd);
            active = outer.get();
        }

        for (const auto& p : j.items())
        {
            const std::string key(p.key());
            const json& val(p.value());

            if (isLogicalOperator(key))
            {
                auto inner(LogicGate::create(key));
                build(*inner, val);
                active->push(std::move(inner));
            }
            else if (!val.is_object() || val.size() == 1)
            {
                // a comparison query object.
                active->push(Comparison::create(m_metadata, key, val));
            }
            else
            {
                // key is the name of a dimension, val is an object of
                // multiple comparison key/val pairs, for example:
                //
                // key: "Red"
                // val: { "$gt": 100, "$lt": 200 }
                //
                // There cannot be any further nested logical operators
                // within val, since we've already selected a dimension.

                for (const auto& inner : val.items())
                {
                    const std::string innerKey(inner.key());
                    const json& innerVal(inner.value());
                    const json next { { innerKey, innerVal } };
                    active->push(Comparison::create(m_metadata, key, next));
                }
            }
        }

        if (outer) gate.push(std::move(outer));
    }

    const Metadata& m_metadata;
    const Bounds m_queryBounds;
    LogicalAnd m_root;
};

} // namespace entwine

