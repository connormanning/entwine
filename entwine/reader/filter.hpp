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

#include <json/json.h>

#include <entwine/reader/comparison.hpp>
#include <entwine/reader/logic-gate.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/metadata.hpp>

namespace entwine
{

class Filter
{
public:
    Filter(
            const Metadata& metadata,
            const Bounds& queryBounds,
            const Json::Value& json,
            const Delta* delta)
        : m_metadata(metadata)
        , m_queryBounds(queryBounds)
        , m_root()
    {
        if (json.isObject()) build(m_root, json, delta);
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
    void build(LogicGate& gate, const Json::Value& json, const Delta* delta)
    {
        if (json.isObject())
        {
            LogicGate* active(&gate);

            std::unique_ptr<LogicGate> outer;

            if (json.size() > 1)
            {
                outer = LogicGate::create(LogicalOperator::lAnd);
                active = outer.get();
            }

            for (const std::string& key : json.getMemberNames())
            {
                const Json::Value& val(json[key]);

                if (isLogicalOperator(key))
                {
                    auto inner(LogicGate::create(key));
                    build(*inner, val, delta);
                    active->push(std::move(inner));
                }
                else if (!val.isObject() || val.size() == 1)
                {
                    // a comparison query object.
                    active->push(
                            Comparison::create(m_metadata, key, val, delta));
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
                    //
                    for (const std::string& innerKey : val.getMemberNames())
                    {
                        Json::Value next;
                        next[innerKey] = val[innerKey];
                        active->push(
                                Comparison::create(
                                    m_metadata,
                                    key,
                                    next,
                                    delta));
                    }
                }
            }

            if (outer) gate.push(std::move(outer));
        }
        else if (json.isArray())
        {
            for (const Json::Value& val : json) build(gate, val, delta);
        }
        else
        {
            throw std::runtime_error(
                    "Unexpected filter type: " + json.toStyledString());
        }
    }

    const Metadata& m_metadata;
    const Bounds m_queryBounds;
    LogicalAnd m_root;
};

} // namespace entwine

